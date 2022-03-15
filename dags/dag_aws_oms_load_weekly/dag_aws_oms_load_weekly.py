# Objeto........: dag_aws_oms_load_weekly
# Version.......: 2.0.0
# Data Criacao..: 23/09/2021
# Projeto.......: MAR
# VS............: Demanda e Abastecimento
# Descricao.....: Dag responsável por executar os jobs da camada trusted/refined (OMS)
# Departamento..: Arquitetura e Engenharia de Dados
# Autor.........: marcus.guidoti@grupoboticario.com.br
# Git: https://github.com/grupoboticario/data-sql/blob/main/dags/dag_aws_oms_load_weekly.py
# ===========================================================================================
# fmt: off

from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from libs.airflow import log
from libs.datafusion_custom_operator import CustomCloudDataFusionStartPipelineOperator

# Load variables
env_var = Variable.get("dag_aws_oms_load_weekly", deserialize_json=True)

# Datafusion variables
instance_name				= env_var["instance_name"]
datafusion_region			= env_var["datafusion_region"]
namespace					= env_var["namespace"]
omsotm_jdbc					= env_var["omsotm_jdbc"]
omsotm_user					= env_var["omsotm_user"]

# Procedures variables
schedule_interval			= env_var["schedule_interval"]
raw_project					= env_var["raw_project"]
raw_custom_project			= env_var["raw_custom_project"]
trusted_project				= env_var["trusted_project"]
refined_project				= env_var["refined_project"]
sensitive_raw_project		= env_var["sensitive_raw_project"]
sensitive_trusted_project	= env_var["sensitive_trusted_project"]
sensitive_refined_project	= env_var["sensitive_refined_project"]
quality_project				= env_var["quality_project"]
retries             		= env_var["retries"]
retry_delay         		= env_var["retry_delay"]
dagrun_timeout				= env_var["dagrun_timeout"]
max_active_runs				= env_var["max_active_runs"]

# Log carga variables
log_carga_project			= env_var["log_carga_project"]
# Data Quality variables
dq_airflow_project			= env_var["dq_airflow_project"]

default_args = {
    "owner": "Ezequiel Moraes - ezequiel.moraes@grupoboticario.com.br",
    "on_failure_callback": log,
    "on_success_callback": log,
    "on_retry_callback": log,
    "sla_miss_callback": log,
    "start_date": datetime(2022, 2, 23),
    "retries": retries,
    "retry_delay": timedelta(minutes=retry_delay),
}

dag = DAG(
    "dag_aws_oms_load_weekly",
    default_args=default_args,
    description= "Job responsavel por enviar dados do BigQuery para AWS semanalmente",
    schedule_interval=schedule_interval,
    dagrun_timeout=timedelta(minutes=dagrun_timeout),
    max_active_runs=max_active_runs
)

####################################
####           TASKS            ####
####################################

# DDV Variables
ddv_pipeline_name 		= "oms_aws_tb_store_sale_forecast"
ddv_dataset_demanda		= "demanda"
ddv_dataset_material	= "material"
ddv_dataset_sellout		= "sellout"
ddv_dataset_auxiliar	= "auxiliar"
ddv_dataset_franqueado	= "franqueado"
ddv_table_log			= "tb_log_pipeline"

df_ddv_to_aws = CustomCloudDataFusionStartPipelineOperator(
    task_id			= ddv_pipeline_name,
    location 		= datafusion_region,
    namespace 		= namespace,
    instance_name 	= instance_name,
    pipeline_name 	= ddv_pipeline_name,
    success_states	= ["COMPLETED"],
    pipeline_timeout= 82800,
    runtime_args	= {
        "project_trusted"			:trusted_project,
        "project_sensitive_trusted" :sensitive_trusted_project,
        "dataset_demanda"			:ddv_dataset_demanda,
        "dataset_material"			:ddv_dataset_material,
        "dataset_sellout"			:ddv_dataset_sellout,
        "dataset_auxiliar"			:ddv_dataset_auxiliar,
        "dataset_franqueado"		:ddv_dataset_franqueado,
        "table_log"					:ddv_table_log,
        "user"						:omsotm_user,
        "connection_string"			:omsotm_jdbc,
    },
    dag=dag,
)

dataquality_trusted_zone_sellout_tb_ciclo_dia = BigQueryOperator(
    task_id="dataquality_trusted_zone_sellout_tb_ciclo_dia",
    bql="""
    CALL `data-quality-gb.sp.prc_dataquality_log_carga`    ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days}','{var_config_log_carga}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_inside_table` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_col_nam}','{var_col_typ}','{var_tolerance_days}','{var_duplicate_key}','{var_config_inside_table}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_origin_target`('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_perc}','{var_config_origin_target}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_airflow`      ('{var_dq_airflow_project}', '{var_dag}', '{var_task}', '{var_tolerance_days}', '{var_config_airflow}', '{var_msg_comp_2}');
    """.format(
        var_dq_airflow_project		= dq_airflow_project,
        var_prj                     = trusted_project,					# projeto
        var_dataset                 = "sellout",						# dataset que se encontra a tabela
        var_table                   = "tb_ciclo_dia",   					# nome da tabela
        var_tolerance_days          = "0",								# tolerância em dias para dataquality_log_carga e inside_table, 0=today, 1= -1 dia. 2=-2dias...
        var_config_log_carga        = "1",								# 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        var_msg_comp_1              = 'Erro dados tabela origem sellout.tb_ciclo_dia. Verificar se houve atualizacao na tabela de origem sellout.tb_ciclo_dia',
        var_col_nam                 = "na",								# nome da coluna que será verificada se os dados estão atualizados (max (colum)>= tolerancia)
        var_col_typ                 = "na",								# tipo, formato dos dados da coluna em questao para o CAST() (2021-09-10 = date), ex: 'date','sem_delta', etc..
        var_duplicate_key           = "cod_ciclo,cod_un_negocio,cod_canal,cod_estrutura_comercial,dt_ciclo_inicio,dt_ciclo_fim",	# verifica se existe chaves duplicadas, se usar mais de uma chave usar ",", ex: 'col1,col2,col3'
        var_config_inside_table     = "na",								# 0= padrão, na=ignora procedure de verificacao.
        var_msg_comp_2              = 'Erro prc_dataquality_airflow',
        var_tolerance_perc 			= "5",
        var_config_origin_target	= "na",
        var_dag                     = "dag_trusted-refined_platform_load_daily",	# nome da dag onde queremos verificar uma task
        var_task                    = "prc_load_tb_ciclo_dia",							# nome da task da outra dag que queremos verificar 
        var_config_airflow          = "na",
    ),
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False,
    retries=0,
)

dataquality_trusted_zone_sellout_tb_dia_ciclo = BigQueryOperator(
    task_id="dataquality_trusted_zone_sellout_tb_dia_ciclo",
    bql="""
    CALL `data-quality-gb.sp.prc_dataquality_log_carga`    ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days}','{var_config_log_carga}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_inside_table` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_col_nam}','{var_col_typ}','{var_tolerance_days}','{var_duplicate_key}','{var_config_inside_table}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_origin_target`('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_perc}','{var_config_origin_target}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_airflow`      ('{var_dq_airflow_project}', '{var_dag}', '{var_task}', '{var_tolerance_days}', '{var_config_airflow}', '{var_msg_comp_2}');
    """.format(
        var_dq_airflow_project		= dq_airflow_project,
        var_prj                     = trusted_project,					# projeto
        var_dataset                 = "sellout",						# dataset que se encontra a tabela
        var_table                   = "tb_dia_ciclo",   				# nome da tabela
        var_tolerance_days          = "0",								# tolerância em dias para dataquality_log_carga e inside_table, 0=today, 1= -1 dia. 2=-2dias...
        var_config_log_carga        = "1",								# 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        var_msg_comp_1              = 'Erro dados tabela origem sellout.tb_dia_ciclo. Verificar se houve atualizacao na tabela de origem sellout.tb_dia_ciclo',
        var_col_nam                 = "na",								# nome da coluna que será verificada se os dados estão atualizados (max (colum)>= tolerancia)
        var_col_typ                 = "na",								# tipo, formato dos dados da coluna em questao para o CAST() (2021-09-10 = date), ex: 'date','sem_delta', etc..
        var_duplicate_key           = "cod_ciclo,cod_dt_ciclo,dt_ciclo,cod_un_negocio,cod_canal,cod_estrutura_comercial,cod_ciclo_ano_anterior",	# verifica se existe chaves duplicadas, se usar mais de uma chave usar ",", ex: 'col1,col2,col3'
        var_config_inside_table     = "na",								# 0= padrão, na=ignora procedure de verificacao.
        var_msg_comp_2              = 'Erro prc_dataquality_airflow',
        var_tolerance_perc 			= "5",
        var_config_origin_target	= "na",
        var_dag                     = "dag_trusted_sellin-sellout_load_daily",	# nome da dag onde queremos verificar uma task
        var_task                    = "prc_load_tb_dia_ciclo",							# nome da task da outra dag que queremos verificar 
        var_config_airflow          = "na",
    ),
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False,
    retries=0,
)

dataquality_trusted_zone_franqueado_tb_franquia = BigQueryOperator(
    task_id="dataquality_trusted_zone_franqueado_tb_franquia",
    bql="""
    CALL `data-quality-gb.sp.prc_dataquality_log_carga`    ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days}','{var_config_log_carga}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_inside_table` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_col_nam}','{var_col_typ}','{var_tolerance_days}','{var_duplicate_key}','{var_config_inside_table}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_origin_target`('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_perc}','{var_config_origin_target}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_airflow`      ('{var_dq_airflow_project}', '{var_dag}', '{var_task}', '{var_tolerance_days}', '{var_config_airflow}', '{var_msg_comp_2}');
    """.format(
        var_dq_airflow_project		= dq_airflow_project,
        var_prj                     = sensitive_trusted_project,		# projeto
        var_dataset                 = "franqueado",						# dataset que se encontra a tabela
        var_table                   = "tb_franquia",   					# nome da tabela
        var_tolerance_days          = "0",								# tolerância em dias para dataquality_log_carga e inside_table, 0=today, 1= -1 dia. 2=-2dias...
        var_config_log_carga        = "1",								# 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        var_msg_comp_1              = 'Erro dados tabela origem franqueado.tb_franquia. Verificar se houve atualizacao na tabela de origem franqueado.tb_franquia',
        var_col_nam                 = "na",								# nome da coluna que será verificada se os dados estão atualizados (max (colum)>= tolerancia)
        var_col_typ                 = "na",								# tipo, formato dos dados da coluna em questao para o CAST() (2021-09-10 = date), ex: 'date','sem_delta', etc..
        var_duplicate_key           = "cod_franquia,cod_un_negocio",	# verifica se existe chaves duplicadas, se usar mais de uma chave usar ",", ex: 'col1,col2,col3'
        var_config_inside_table     = "na",								# 0= padrão, na=ignora procedure de verificacao.
        var_msg_comp_2              = 'Erro prc_dataquality_airflow',
        var_tolerance_perc 			= "5",
        var_config_origin_target	= "na",
        var_dag                     = "dag_trusted-refined_platform_load_daily",	# nome da dag onde queremos verificar uma task
        var_task                    = "prc_load_tb_franquia",							# nome da task da outra dag que queremos verificar 
        var_config_airflow          = "na",
    ),
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False,
    retries=0,
)

dataquality_trusted_zone_material_tb_material = BigQueryOperator(
    task_id="dataquality_trusted_zone_material_tb_material",
    bql="""
    CALL `data-quality-gb.sp.prc_dataquality_log_carga`    ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days}','{var_config_log_carga}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_inside_table` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_col_nam}','{var_col_typ}','{var_tolerance_days}','{var_duplicate_key}','{var_config_inside_table}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_origin_target`('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_perc}','{var_config_origin_target}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_airflow`      ('{var_dq_airflow_project}', '{var_dag}', '{var_task}', '{var_tolerance_days}', '{var_config_airflow}', '{var_msg_comp_2}');
    """.format(
        var_dq_airflow_project		= dq_airflow_project,
        var_prj                     = trusted_project,					# projeto
        var_dataset                 = "material",						# dataset que se encontra a tabela
        var_table                   = "tb_material",   					# nome da tabela
        var_tolerance_days          = "0",								# tolerância em dias para dataquality_log_carga e inside_table, 0=today, 1= -1 dia. 2=-2dias...
        var_config_log_carga        = "1",								# 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        var_msg_comp_1              = 'Erro dados tabela origem material.tb_material. Verificar se houve atualizacao na tabela de origem material.tb_material',
        var_col_nam                 = "na",								# nome da coluna que será verificada se os dados estão atualizados (max (colum)>= tolerancia)
        var_col_typ                 = "na",								# tipo, formato dos dados da coluna em questao para o CAST() (2021-09-10 = date), ex: 'date','sem_delta', etc..
        var_duplicate_key           = "cod_material,cod_un_material",	# verifica se existe chaves duplicadas, se usar mais de uma chave usar ",", ex: 'col1,col2,col3'
        var_config_inside_table     = "na",								# 0= padrão, na=ignora procedure de verificacao.
        var_msg_comp_2              = 'Erro prc_dataquality_airflow',
        var_tolerance_perc 			= "5",
        var_config_origin_target	= "na",
        var_dag                     = "dag_trusted_material_load_daily",	# nome da dag onde queremos verificar uma task
        var_task                    = "prc_load_tb_material",			# nome da task da outra dag que queremos verificar 
        var_config_airflow          = "0",
    ),
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False,
    retries=0,
)

dataquality_trusted_zone_sellout_tb_real_dia_cupom_so = BigQueryOperator(
    task_id="dataquality_trusted_zone_sellout_tb_real_dia_cupom_so",
    bql="""
    CALL `data-quality-gb.sp.prc_dataquality_log_carga`    ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days}','{var_config_log_carga}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_inside_table` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_col_nam}','{var_col_typ}','{var_tolerance_days}','{var_duplicate_key}','{var_config_inside_table}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_origin_target`('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_perc}','{var_config_origin_target}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_airflow`      ('{var_dq_airflow_project}', '{var_dag}', '{var_task}', '{var_tolerance_days}', '{var_config_airflow}', '{var_msg_comp_2}');
    """.format(
        var_dq_airflow_project		= dq_airflow_project,
        var_prj                     = trusted_project,					# projeto
        var_dataset                 = "sellout",						# dataset que se encontra a tabela
        var_table                   = "tb_real_dia_cupom_so",			# nome da tabela
        var_tolerance_days          = "0",								# tolerância em dias para dataquality_log_carga e inside_table, 0=today, 1= -1 dia. 2=-2dias...
        var_config_log_carga        = "1",								# 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        var_msg_comp_1              = 'Erro dados tabela origem sellout.tb_real_dia_cupom_so. Verificar se houve atualizacao na tabela de origem sellout.tb_real_dia_cupom_so',
        var_col_nam                 = "na",								# nome da coluna que será verificada se os dados estão atualizados (max (colum)>= tolerancia)
        var_col_typ                 = "na",								# tipo, formato dos dados da coluna em questao para o CAST() (2021-09-10 = date), ex: 'date','sem_delta', etc..
        var_duplicate_key           = "na",								# verifica se existe chaves duplicadas, se usar mais de uma chave usar ",", ex: 'col1,col2,col3'
        var_config_inside_table     = "na",								# 0= padrão, na=ignora procedure de verificacao.
        var_msg_comp_2              = 'Erro prc_dataquality_airflow',
        var_tolerance_perc 			= "5",
        var_config_origin_target	= "na",
        var_dag                     = "dag_trusted_sellin-sellout_load_daily",	# nome da dag onde queremos verificar uma task
        var_task                    = "prc_load_tb_real_dia_cupom_so",			# nome da task da outra dag que queremos verificar 
        var_config_airflow          = "0",
    ),
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False,
    retries=0,
)

dataquality_sensitive_trusted_demanda_tb_phase_in_out_so = BigQueryOperator(
    task_id="dataquality_sensitive_trusted_demanda_tb_phase_in_out_so",
    bql="""
    CALL `data-quality-gb.sp.prc_dataquality_log_carga`    ('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_days}','{var_config_log_carga}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_inside_table` ('{var_prj}', '{var_dataset}', '{var_table}', '{var_col_nam}','{var_col_typ}','{var_tolerance_days}','{var_duplicate_key}','{var_config_inside_table}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_origin_target`('{var_prj}', '{var_dataset}', '{var_table}', '{var_tolerance_perc}','{var_config_origin_target}','{var_msg_comp_1}');
    CALL `data-quality-gb.sp.prc_dataquality_airflow`      ('{var_dq_airflow_project}', '{var_dag}', '{var_task}', '{var_tolerance_days}', '{var_config_airflow}', '{var_msg_comp_2}');
    """.format(
        var_dq_airflow_project		= dq_airflow_project,
        var_prj                     = sensitive_trusted_project,		# projeto
        var_dataset                 = "demanda",						# dataset que se encontra a tabela
        var_table                   = "tb_phase_in_out_so",   			# nome da tabela
        var_tolerance_days          = "0",								# tolerância em dias para dataquality_log_carga e inside_table, 0=today, 1= -1 dia. 2=-2dias...
        var_config_log_carga        = "1",								# 0= padrão, consulta auxiliar.tb_log_carga, 1=verifica diretamente no status da tabela.
        var_msg_comp_1              = 'Erro dados tabela origem demanda.tb_phase_in_out_so. Verificar se houve atualizacao na tabela de origem demanda.tb_phase_in_out_so',
        var_col_nam                 = "na",								# nome da coluna que será verificada se os dados estão atualizados (max (colum)>= tolerancia)
        var_col_typ                 = "na",								# tipo, formato dos dados da coluna em questao para o CAST() (2021-09-10 = date), ex: 'date','sem_delta', etc..
        var_duplicate_key           = "na",								# verifica se existe chaves duplicadas, se usar mais de uma chave usar ",", ex: 'col1,col2,col3'
        var_config_inside_table     = "na",								# 0= padrão, na=ignora procedure de verificacao.
        var_msg_comp_2              = 'Erro prc_dataquality_airflow',
        var_tolerance_perc 			= "5",
        var_config_origin_target	= "na",
        var_dag                     = "dag_trusted-refined_mar-multimarcas_load_daily",# nome da dag onde queremos verificar uma task
        var_task                    = "prc_load_tb_phase_in_out_so_sensitive",			# nome da task da outra dag que queremos verificar 
        var_config_airflow          = "0",
    ),
    use_legacy_sql=False,
    priority="BATCH",
    dag=dag,
    depends_on_past=False,
    retries=0,
)

"""
EXECUÇÃO TASKS
"""
[
    dataquality_trusted_zone_sellout_tb_ciclo_dia,
    dataquality_trusted_zone_sellout_tb_dia_ciclo,
    dataquality_trusted_zone_franqueado_tb_franquia,
    dataquality_trusted_zone_material_tb_material,
    dataquality_trusted_zone_sellout_tb_real_dia_cupom_so,
    dataquality_sensitive_trusted_demanda_tb_phase_in_out_so	
] >> df_ddv_to_aws
