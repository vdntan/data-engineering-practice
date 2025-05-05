from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='exercises_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Run exercises 1-5 using Docker containers',
) as dag:

    build_images = BashOperator(
        task_id='build_images',
        bash_command='{{ "bash /opt/airflow/build_images.sh" }}',
    )

    run_ex1 = DockerOperator(
        task_id='run_exercise_1',
        image='exercise-1',
        command='python main.py',
        docker_url='unix:///var/run/docker.sock',
        #network_mode='bridge',
    )

    run_ex2 = DockerOperator(
        task_id='run_exercise_2',
        image='exercise-2',
        command='python main.py',
        docker_url='unix:///var/run/docker.sock',
        #network_mode='bridge',
    )

    run_ex3 = DockerOperator(
        task_id='run_exercise_3',
        image='exercise-3',
        command='python main.py',
        docker_url='unix:///var/run/docker.sock',
        #network_mode='bridge',
    )

    run_ex4 = DockerOperator(
        task_id='run_exercise_4',
        image='exercise-4',
        command='python main.py',
        docker_url='unix:///var/run/docker.sock',
        #network_mode='bridge',
    )

    run_ex5 = DockerOperator(
        task_id='run_exercise_5',
        image='exercise-5',
        command='python main.py',
        docker_url='unix:///var/run/docker.sock',
        network_mode='data-engineering-practice_airflow_net',  # <<< Quan trọng
        #auto_remove=True,
        mount_tmp_dir=False,
    )

    build_images >> run_ex1 >> run_ex2 >> run_ex3 >> run_ex4 >> run_ex5
