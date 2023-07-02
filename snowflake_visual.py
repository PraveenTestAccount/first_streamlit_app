# streamlit pack
import streamlit as st

# other python packs
import pandas as pd
import pandas as pd
import datetime as dt
#import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
# snowpark packs
from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
from snowflake.snowpark import version
#snowflake connection info is saved in config.py
from config import snowflake_conn_prop
# lets import some tranformations functions
from snowflake.snowpark.functions import when, udf, col, lit, translate, is_null, iff
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode

st.set_page_config(layout="wide")
hide_menu_style = """
        <style>
        #MainMenu {visibility: hidden;}
        body { background-color: #f9f9f9}
        </style>
        """
st.markdown(hide_menu_style, unsafe_allow_html=True)

top_container = st.container()
user_access_container= st.container()
credit_usage_container= st.container()

with top_container:
    st.title('Health Check')
    st.text('This app showcases different aspects of snowflake health check: user access and analysis, credit usage and consumption,')   
    st.text('This app utilizes snowpark, pandas and streamlit')
    st.text(f'Snowpark version: {version.VERSION}')
    st.text('First we create a Snowpark session by connecting to your Snowflake account. This is possible using Session class lets you build a session with your creds to snowflake.Snowpark has many functions to interact with the data and let you convert Snowpark data frame to Pandas or vice versa for more analytical or exploratory analysis')
    session = Session.builder.configs(snowflake_conn_prop).create()

    rolename = "ACCOUNTADMIN"
    dbname = "SNOWFLAKE"
    schemaname = "ACCOUNT_USAGE"
    warehouse = "XSMALL"

    session.sql(f"USE ROLE {rolename}").collect()
    #session.sql(f"CREATE DATABASE IF NOT EXISTS {dbname}").collect()
    #session.sql(f"CREATE SCHEMA IF NOT EXISTS {dbname}.{schemaname}").collect()
    session.sql(f"CREATE WAREHOUSE  IF NOT EXISTS {warehouse} \
                    WAREHOUSE_SIZE = 'XSMALL' \
                    AUTO_SUSPEND = 300 \
                    AUTO_RESUME = TRUE \
                    MIN_CLUSTER_COUNT = 1 \
                    MAX_CLUSTER_COUNT = 2 \
                    SCALING_POLICY = 'STANDARD' ").collect()
    session.sql(f"USE WAREHOUSE {warehouse}").collect()
    session.sql(f"USE SCHEMA {dbname}.{schemaname}").collect()
    st.text('Testing connection by printing the warehouse, database and schema set in the current session')
    session_info_list = session.sql('select current_warehouse(), current_database(), current_schema()').collect()
    st.text(f'Session info: {session_info_list}')

page = st.sidebar.selectbox('Select page', ['User access and management','Credit usage and consumption'])

if page == 'User access and management':
    with user_access_container:
        st.header('User access and management')
        st.text('Following sections highlight user accounts and their analysis ranging from details on who is active, latest logins, errors and most common causes')
        # users_list_df = session.sql('''
        # select 
        #     display_name
        #     , last_name 
        #     , case 
        #         when deleted_on is not null then 'DELETED'
        #         when deleted_on is null and disabled then 'DISABLED'
        #         when deleted_on is null and snowflake_lock then 'LOCKED'
        #         else 'ACTIVE'
        #     end as status 
        #     , datediff(day, password_last_set_time::timestamp_ntz, current_timestamp) as password_changed_before_days
        #     , datediff(day, last_success_login::timestamp_ntz, current_timestamp) as last_login_before_days
        # from snowflake.account_usage.users;''').toPandas()
        # lengths_ = users_list_df['DISPLAY_NAME'].map(lambda x: len(x))
        # users_list_df['DISPLAY_NAME'] = users_list_df['DISPLAY_NAME'].str[0:4]+ ["*"*i for i in lengths_]

        # st.subheader('Users deleted, disabled and locked in snowflake') 
        # st.write( users_list_df[users_list_df['STATUS'].isin(['DELETED','DISABLED', 'LOCKED'])] [['DISPLAY_NAME','STATUS']])

        # st.subheader('Users who have not changed password during the last 60 days and beyond')
        # st.write(users_list_df[users_list_df['STATUS'].isin(['LOCKED', 'ACTIVE']) & (users_list_df['PASSWORD_CHANGED_BEFORE_DAYS']>60.0)]  [['DISPLAY_NAME','STATUS','PASSWORD_CHANGED_BEFORE_DAYS']])

        # st.subheader('Users who have not logged in the last 30 days')
        # st.write(users_list_df[users_list_df['STATUS'].isin(['LOCKED', 'ACTIVE']) & (users_list_df['LAST_LOGIN_BEFORE_DAYS']>30.0)]  [['DISPLAY_NAME','STATUS','LAST_LOGIN_BEFORE_DAYS']])

        # sql_query = session.sql('''
        # select distinct 
        #     table_catalog as database
        #     , privilege
        #     --listagg(privilege, ', ') within group (order by privilege desc) as privilege
        #     , usr.display_name as display_name
        #     --u.role as via_role_assigned_to_user,
        #     , r.granted_on as object_type
        # from  
        #     snowflake.account_usage.grants_to_users u 
        # inner join 
        #     snowflake.account_usage.users usr 
        #     on usr.login_name = u.grantee_name
        # inner join 
        #     snowflake.account_usage.grants_to_roles r 
        #     on u.role = r.grantee_name and u.deleted_on is null and r.deleted_on is null
        #         and r.granted_on in ('DATABASE') order by table_catalog, privilege''')
        # users_db_access_df = sql_query.select(col('DATABASE'), col('PRIVILEGE'), col('DISPLAY_NAME')).toPandas()
        # lengths_ = users_db_access_df['DISPLAY_NAME'].map(lambda x: len(x))
        # users_db_access_df['DISPLAY_NAME'] = users_db_access_df['DISPLAY_NAME'].str[0:4]+ ["*"*i for i in lengths_]

        # st.subheader('Users who have the selected privileges')
        # privleges_list = users_db_access_df['PRIVILEGE'].unique()
        # privilege = st.selectbox("Privilege", sorted(privleges_list), index=0)
        # database = st.selectbox("Database", sorted(users_db_access_df.loc[users_db_access_df['PRIVILEGE'] == privilege]['DATABASE'].unique()))
        # filtered_df = users_db_access_df[users_db_access_df['DATABASE']==database][users_db_access_df['PRIVILEGE']==privilege]
        # st.write(filtered_df[['DISPLAY_NAME']])
        
        sql_query = session.sql('''
            select 
                error_message
                , count(*) as failed_attempts
            from    
                snowflake.account_usage.login_history 
            where 
                is_success='NO'
            group by error_message
            order by failed_attempts desc, error_message''')
        failed_attempts_df = sql_query.toPandas()

        st.subheader('Various Failed attempts by type')
        #fig = px.line(failed_attempts_df, x = 'error_message', y = "failed_attempts", title = 'Failed Attempts')
        fig = px.bar(failed_attempts_df, x='ERROR_MESSAGE', y='FAILED_ATTEMPTS')
        st.plotly_chart(fig)
        #st.write(failed_attempts_df)

elif page == 'Credit usage and consumption':
    with credit_usage_container:
        st.header('Credit usage and consumption')
        st.text('Following topics are focused on database and table storage metrics and query usage adn credit consumption.')

        # table_mostactive_df = session.sql('''
        # with usage as (
        #     select 
        #         obj.value:objectName::string table_name
        #         , obj.value:objectId::string table_id
        #         , count(*) uses
        #     from snowflake.account_usage.access_history 
        #         , table(flatten(direct_objects_accessed)) obj
        #     where table_name is not null
        #     group by 1,2
        #     order by uses DESC
        #     limit 25)
        #     select 
        #         usage.table_name,
        #         usage.table_id,
        #         usage.uses,
        #         tsm.active_bytes
        #     from usage
        #     left join snowflake.account_usage.table_storage_metrics tsm
        #     on usage.table_id = tsm.id
        #     order by uses desc''').toPandas()
        # st.subheader('Most active tables and their corresponding size')
        # st.write(table_mostactive_df[table_mostactive_df['ACTIVE_BYTES'].notnull()])

        warehouse_health_df = session.sql('''
        with cte as (
            select
                warehouse_name || ' - ' || warehouse_size as warehouse
                , sum(execution_time)/1000 as execution_time
                , sum(queued_provisioning_time)/1000 as queued_provisioning_time
                , sum(queued_overload_time)/1000 as queued_overload_time
                , date(start_time) as start_date
            from snowflake.account_usage.query_history 
            where warehouse is not null and end_time > dateadd(day, -30, current_timestamp ) group by warehouse, start_date
        )
        select 
            warehouse
            , start_date 
            , ceil(avg(execution_time)
                    OVER(ORDER BY warehouse, start_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
                                                                                    as mavg_execution_time_sec 
            , ceil(avg(queued_provisioning_time)
                    OVER(ORDER BY warehouse, start_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
                                                                                    as mavg_queued_provisioning_time_sec 
            , ceil(avg(queued_overload_time)
                    OVER(ORDER BY warehouse, start_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
                                                                                    as mavg_queued_overload_time_sec
            from cte order by warehouse, start_date desc''').toPandas()
        st.subheader('Warehouse Health - 3 day moving average')
        st.text('This shows the selected warehouse\'s execution time, queued provisioning time and overload time spend on queries in the last 30 days obtained from snowfalke query history')
        # warehouse_list = warehouse_health_df['WAREHOUSE'].unique()
        # warehouse = st.selectbox("Warehouse", sorted(warehouse_list), index=0)
        # filtered_df = warehouse_health_df[warehouse_health_df['WAREHOUSE']==warehouse]
        # fig = px.bar(filtered_df, x='START_DATE', y='MAVG_EXECUTION_TIME_SEC', title='Warehouse execution time')
        # st.plotly_chart(fig)
        # fig = px.bar(filtered_df, x='START_DATE', y='MAVG_QUEUED_PROVISIONING_TIME_SEC', title='Warehouse queued provisioning time')
        # st.plotly_chart(fig)
        # fig = px.bar(filtered_df, x='START_DATE', y='MAVG_QUEUED_OVERLOAD_TIME_SEC', title='Warehouse queued overload time')
        # st.plotly_chart(fig)

        #https://towardsdatascience.com/make-dataframes-interactive-in-streamlit-c3d0c4f84ccb
        AgGrid(warehouse_health_df)
        # gb = GridOptionsBuilder.from_dataframe(warehouse_health_df)
        # gb.configure_pagination(paginationAutoPageSize=True) #Add pagination
        # gb.configure_side_bar() #Add a sidebar
        # gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
        # gridOptions = gb.build()

        # grid_response = AgGrid(
        #     warehouse_health_df,
        #     gridOptions=gridOptions,
        #     data_return_mode='AS_INPUT', 
        #     update_mode='MODEL_CHANGED', 
        #     fit_columns_on_grid_load=False,
        #     theme='blue', #Add theme color to the table
        #     enable_enterprise_modules=True,
        #     height=350, 
        #     width='100%',
        #     reload_data=True
        # )

        # data = grid_response['data']
        # selected = grid_response['selected_rows'] 
        # df = pd.DataFrame(selected) 
