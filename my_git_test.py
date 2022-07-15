
from datetime import timedelta
from lib2to3.pygram import python_symbols

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException, AirflowSkipException

from airflow.models import Variable

from random import randint

D_bases = ['A', 'T', 'G', 'C']

args = {
    'owner': 'airflow',
}

def _result_report(**context):
    summary_data = context['ti'].xcom_pull(task_ids=
                'collect_summary_model')
    trend_data = context['ti'].xcom_pull(task_ids=
                'collect_trend_model')
    max_summary = [k for k,v in summary_data.items()
                    if max(summary_data.values()) == v]
    max_trend = [k for k,v in trend_data.items()
                    if max(trend_data.values()) == v]    
    return '{}, {}'.format(max_summary, max_trend)

def _collect_trend_model(**context):
    idx_size = int(Variable.get('NUM_TRAINING'))
    datas = [context['ti'].xcom_pull(task_ids=
            'trend_model_'+str(i)) for i in range(1, idx_size)]
    result = {}
    for i in range(idx_size-1):
        for data in datas[i]:
            if data not in result.keys():
                result[data] = 1
            else:
                result[data] += 1
    return result

def _collect_summary_model(**context):
    idx_size = int(Variable.get('NUM_TRAINING'))
    datas = [context['ti'].xcom_pull(task_ids=
            'summary_model_'+str(i)) for i in range(1, idx_size)]
    
    result = {'A->T':0, 'A->G':0, 'A->C':0,
          'T->A':0, 'T->G':0, 'T->C':0,
          'G->A':0, 'G->T':0, 'G->C':0,
          'C->A':0, 'C->T':0, 'C->G':0,
          'unchanged':0}    
    for i in range(idx_size-1):
        for key in result.keys():
            result[key] += datas[i][key]
    return result

def _change_trend_model(**context):
    idx = context["params"]["idx"]
    datas = context['ti'].xcom_pull(task_ids=
        'numeric_model_'+str(idx),
    )
    
    result = [i for i, data in enumerate(datas) if data!=0]
    return result

def _change_summary_model(**context):
    idx = context["params"]["idx"]
    datas = context['ti'].xcom_pull(task_ids=
        'numeric_model_'+str(idx),
    )
    
    result = {'A->T':0, 'A->G':0, 'A->C':0,
          'T->A':0, 'T->G':0, 'T->C':0,
          'G->A':0, 'G->T':0, 'G->C':0,
          'C->A':0, 'C->T':0, 'C->G':0,
          'unchanged':0}
    for data in datas:
        value = data&7
        key=''
        if(data&8):
            value-=1
            if(not value&4):
                key+='C'
            elif(not value&2):
                key+='G'
            else:
                key+='T'
            key+='->'
            if(not value&1):
                key+='A'
            elif(not value&2):
                key+='T'
            else:
                key+='G'
        elif(data):
            if(value&1):
                key+='A'
            elif(value&2):
                key+='T'
            else:
                key+='G'
            key+='->'
            if(value&4):
                key+='C'
            elif(value&2):
                key+='G'
            else:
                key+='T'
        else:
            key='unchanged'
        result[key]+=1
    return result


def _error_detected(**context):
    idx = context["params"]["idx"]
    raise AirflowException("error detected in data[{}]".format(idx))

def _error_detect_model(**context):
    idx = context["params"]["idx"]
    idx_size = int(Variable.get('NUM_TRAINING'))
    dna_size = int(Variable.get('DNA_SIZE'))
    data = context['ti'].xcom_pull(task_ids=
        'training_model_'+str(idx),
    )
    for i in range(dna_size):
        check = data[i]
        count = 0
        for j in range(4):
            if(check==D_bases[j]):
                count+=1
        if(count==4):
            return 'error_detected'+idx
    if(idx==0):
        return 'numeric_model_1'
    elif(idx==idx_size-1):
        return 'numeric_model_'+str(idx)
    else:
        return ['numeric_model_'+str(idx), 'numeric_model_'+str(idx+1)]
    

def _numeric_model(**context):
    idx = context["params"]["idx"]
    dna_size = int(Variable.get('DNA_SIZE'))
    datas = context['ti'].xcom_pull(task_ids=[
        'training_model_' + str(idx-1),
        'training_model_' + str(idx),
    ])
    numvalue = {'A':0, 'T':1, 'G':3, 'C':7}
    result = [numvalue[datas[1][i]]-numvalue[datas[0][i]] for i in range(dna_size)]
    return result

def _training_model(**context):
    idx = context["params"]["idx"]
    data = context['ti'].xcom_pull(key=
        'DNA_value',
    )
    return data[idx]

def _check_model_error(**context):
    if(Variable.get("test_run")!='0'):
        return 
        
    models = Variable.get('input_DNA_data')
    idx_size = int(Variable.get('NUM_TRAINING'))
    dna_size = int(Variable.get('DNA_SIZE'))   
    if(len(models)!=idx_size*dna_size):
        raise  AirflowSkipException("input_DNA_data{} is not equal to NUM_TRAINING * DNA_SIZE".format(len(models)))
    return False


def _call_random(**context):
    result=[]
    idx_size = int(Variable.get('NUM_TRAINING'))
    dna_size = int(Variable.get('DNA_SIZE'))
    p_of_mutate = int(Variable.get('P_OF_MUTATE'))
    seed=[randint(0,3) for i in range(dna_size)]
    for i in range(idx_size):
        model=['' for j in range(dna_size)]
        for k, b in enumerate(seed):
            model[k]=D_bases[int(b)]
            if(randint(1,100)<=p_of_mutate):
                original = seed[k]
                while(True):
                    change = randint(0,3)
                    if(change != original):
                        seed[k] = change
                        break
        result.append(model)
        
    context['ti'].xcom_push(key='DNA_value', value = result)
    return result
    
def _call_training(**context):
    result = []
    models = Variable.get('input_DNA_data')
    idx_size = int(Variable.get('NUM_TRAINING'))
    dna_size = int(Variable.get('DNA_SIZE'))    
    for i in range(idx_size):
        model = list(models[10*i:10*i+dna_size])
        result.append(model)
    context['ti'].xcom_push(key='DNA_value', value = result)
    return result

def _check_type():
    value = Variable.get("test_run")
    if(value!='0'):
        return 'call_random_model'
    return 'call_training_model'

with DAG(
    dag_id='a_mymy_gittest',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:

    #run_this_last = DummyOperator(
    #    task_id='run_this_last',
    #)

    check_model_type = BranchPythonOperator(
        task_id='check_model_type',
        python_callable=_check_type,
    )

    call_random_model = PythonOperator(
        task_id='call_random_model',
        python_callable=_call_random,
    )
    call_training_model = PythonOperator(
        task_id='call_training_model',
        python_callable=_call_training,
    )
    check_model = PythonOperator(
        task_id='check_model_error',
        python_callable=_check_model_error,
        trigger_rule='one_success'
    )
    check_model_type >> [call_random_model, call_training_model] >> check_model
    
    collect_trend_model = PythonOperator(
        task_id='collect_trend_model',
        python_callable=_collect_trend_model,
    )
    collect_summary_model = PythonOperator(
        task_id='collect_summary_model',
        python_callable=_collect_summary_model,
    )

    for i in range(int(Variable.get('NUM_TRAINING'))):
        training_task = PythonOperator(
            task_id='training_model_' + str(i),
            python_callable=_training_model,
            params={'idx':i},
            provide_context=True,
        )
        check_model >> training_task

        if(i!=0):
            change_to_num = PythonOperator(
                task_id='numeric_model_' + str(i),
                python_callable=_numeric_model,
                params={'idx':i},
                provide_context=True,
            )
            error_detect >> change_to_num
            summary_task = PythonOperator(
               task_id='summary_model_' + str(i),
               python_callable=_change_summary_model,
               params={'idx':i},
               provide_context=True,
            )
            change_to_num >> summary_task >> collect_summary_model
            trend_task = PythonOperator(
                task_id='trend_model_' +str(i),
               python_callable=_change_trend_model,
               params={'idx':i},
               provide_context=True,
            )
            change_to_num >> trend_task >> collect_trend_model
        
        error_detect = BranchPythonOperator(
            task_id='error_detect_' + str(i),
            python_callable=_error_detect_model,
            params={'idx':i},
            provide_context=True,
        )
        
        error_detected = PythonOperator(
            task_id='error_detected_'+str(i),
            python_callable=_error_detected,
        )
        training_task >> error_detect >> error_detected
        if(i!=0):
            error_detect >> change_to_num            

    result_report = PythonOperator(
        task_id='result_report',
        python_callable=_result_report,
    )
    [collect_summary_model, collect_trend_model] >> result_report
    # [START howto_operator_bash_template]

    # [END howto_operator_bash_template]
    

# [START howto_operator_bash_skip]
#this_will_ = BashOperator(
#    task_id='this_will_skip',
#    bash_command='echo "hello world"; exit 99;',
#    dag=dag,
#)
# [END howto_operator_bash_skip]
#this_will_ >> run_this_last

if __name__ == "__main__":
    dag.cli()

    