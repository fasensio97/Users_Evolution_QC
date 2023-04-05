#!/usr/bin/env python
# coding: utf-8

# ## Importamos librerias

# In[27]:


import pandas as pd
import numpy as np
from sqlalchemy import *
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
from dateutil.relativedelta import relativedelta
from df2gspread import df2gspread as d2g
#import unidecode
import pandas_gbq
from google.cloud import bigquery
from google.cloud import bigquery_storage
import google.auth
import pydata_google_auth
import time


# ## Acceso a credenciales OK

# In[28]:


#Accedemos a las credenciales
scope = ['https://www.googleapis.com/auth/drive.file']
#ruta y nombre del acceso
credentials = ServiceAccountCredentials.from_json_keyfile_name('PedidosYa-6e661fd93faf.json', scope)
gc = gspread.authorize(credentials)


# # Bajada queries

# In[37]:


#Definimos las verticales (Market ya está dividido en supermercado y comercios especializados)
verticales = ['Local Stores', 'QC','Market','Pharmacy','Drinks','DMarts','Kiosks']
#verticales = ['DMarts']


# Definimos la cantidad de iteraciones que queremos hacer
num_iterations = 4

# último mes que quiero analizar, en el caso que sea marzo datetime.date(2023, 3, 1)
from_date = datetime.date(2023, 2, 1)



#desde que fecha no tiene que tener órdenes para reconsiderarlo una nueva compra?
from_date_new_users = datetime.date(2021, 1, 1)

############################ Desde acá no modificar nada ###########################################################3

# Definimos la cantidad de meses que queremos restar en cada iteración
months_to_subtract = 1


#Creamos el df para almacenar la info de las queries
column_names = ['Month', 'vertical', 'q_user_voucher', 'Vouchers_CRM', 'q_user_df0', 
                'q_user_corporate', 'q_user_banks', 'q_user_plus', 'q_user_totales', 
                'q_orders_totales', 'q_users_incentive', 'q_orders_incentive']

df = pd.DataFrame(columns=column_names)

#corremos el bucle
i=0
for i in range(num_iterations):

    # Restamos los meses correspondientes a la fecha de inicio
    if i == 0:
        from_date -= relativedelta(months=i)
        from_date = from_date.replace(day=1)
    else:
        from_date -= relativedelta(months=1)
        from_date = from_date.replace(day=1)
    
    # Definimos la fecha de fin como el último día del mes de la fecha de inicio
    to_date = from_date.replace(day=1) + relativedelta(months=1) - datetime.timedelta(days=1)
    
    # Definimos la fecha de inicio del mes anterior a la fecha de fin
    from_date_lm = (to_date - relativedelta(months=1)).replace(day=1)
    
    # Definimos la fecha de fin del mes anterior a la fecha de fin
    to_date_lm = from_date.replace(day=1) - datetime.timedelta(days=1)
    
    for v in verticales:


                # Definimos la variable de vertical
            vertical = v

                # Imprimimos todas las fechas
            print('corriendo query del: {} hasta {}, vertical {}'.format(from_date, to_date,vertical))

            if vertical == 'DMarts':
                           query = f'''
                with new_users_tm_qc as(	

                                SELECT
                                '{vertical}' AS tipo,
                                fo.user.id,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3

                                AND dp.is_darkstore

                                --No compraron en la vertical en los últimos 12 meses
                                AND fo.user.id NOT IN (SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	
                                WHERE fo.registered_date BETWEEN '{from_date_new_users}' AND '{to_date_lm}' AND fo.country_id = 3 
                                AND fo.order_status = 'CONFIRMED'
                                AND dp.is_darkstore


                                GROUP BY 1)
                                AND fo.user.id NOT IN(SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` du ON fo.user.id = du.user_id
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id
                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.data.firstSuccessful
                                AND dp.is_darkstore

                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                GROUP BY 1)	

                                GROUP BY 1,2	


                                ),

                acquisitions_tm as(	
                                SELECT
                                fo.user.id as user_id,	
                                '{vertical}' AS tipo,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	

                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                AND fo.data.firstSuccessful	

                                AND dp.is_darkstore

                                GROUP BY 1,2	

                                ),



                reactivated_tm_qc as(    

                            SELECT
                            DISTINCT fo.user.id

                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id 
                            LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id	
                            LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id

                            WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')    
                            AND fo.country_id = 3    
                            AND fo.order_status = 'CONFIRMED'    

                            AND dp.is_darkstore


                            AND fo.user.id NOT IN    
                            (SELECT    
                            fo.user.id AS user_id    
                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id    
                            WHERE fo.registered_date BETWEEN DATE('{from_date_lm}') AND DATE('{to_date_lm}')
                            AND fo.country_id = 3 
                            AND fo.order_status = 'CONFIRMED'    

                            AND dp.is_darkstore


                            GROUP BY 1)    

                            AND acquisitions_tm.user_id is null
                            AND new_users_tm_qc.id is null   

                            GROUP BY 1    


                )

                SELECT	

                  FORMAT_DATETIME('%Y-%m',fo.registered_date) AS Month,
                  '{vertical}' AS vertical,
                    CASE WHEN reactivated_tm_qc.id is not null then 'Reactivados'
                  WHEN acquisitions_tm.user_id is not null then 'Acquisition'
                  WHEN new_users_tm_qc.id is not null then 'New Users'
                  END AS user_type,

                  --COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND p.fromDate IS NOT NULL AND p.toDate IS NOT NULL THEN fo.user.id END) AS q_orders_campana,

                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.has_voucher_discount > 0 THEN fo.user.id END) AS q_user_voucher,
                  count(distinct case when fo.order_status ='CONFIRMED' and (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) then fo.user.id end ) as Vouchers_CRM,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true THEN fo.user.id END) AS q_user_df0,
                  (COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') THEN fo.user.id END)) AS q_user_corporate,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND c.name IS NOT NULL THEN fo.user.id END) AS q_user_banks,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.is_user_plus = 1 THEN fo.user.id END) AS q_user_plus,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.user.id END) AS q_user_totales,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.order_id END) AS q_orders_totales,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.user.id END) AS q_users_incentive,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.order_id END) AS q_orders_incentive,

                  FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                  LEFT JOIN reactivated_tm_qc ON fo.user.id = reactivated_tm_qc.id	
                  LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id	
                  LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id      
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` dp ON dp.partner_id = fo.restaurant.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS dim_city__partner ON dp.city_id = dim_city__partner.city_id
                  LEFT JOIN UNNEST([dp.franchise]) as dp_franchise
                  LEFT JOIN UNNEST (fo.details) as fo_details
                  LEFT JOIN UNNEST (fo.discounts) as fo_discounts
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_product` AS dim_product ON fo_details.product.product_id = dim_product.product_id
                  LEFT JOIN `peya-data-origins-pro.cl_core.product` p ON dim_product.product_id = p.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` dsp ON fo_details.product.product_id = dsp.product_id AND fo_details.is_subsidized = true
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` dsc ON dsp.subsidized_product_campaing_id = dsc.subsidized_campaign_id
                  LEFT JOIN `peya-bi-tools-pro.il_wallet.fact_wallet` fw ON fo.order_id = fw.order_id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` ftc ON (fo.order_id = ftc.order_id AND DATE(ftc.coupon_created_at) > DATE_ADD(CURRENT_DATE(), INTERVAL -100 DAY))
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` dtc ON ftc.talon_campaign_id = dtc.talon_campaign_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.order_log` ol ON fo.order_id = ol.order_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign_log_items` cli ON ol.id = cli.order_log_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign` c ON cli.id = c.id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id

                WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')

                AND fo.country_id = 3	
                AND (reactivated_tm_qc.id IS NOT NULL	OR acquisitions_tm.user_id IS NOT NULL OR new_users_tm_qc.id is not null)

                GROUP BY 1,2,3'''
                    
            elif vertical == 'Market':
                query = f'''
                
                with new_users_tm_qc as(	

                                SELECT
                                '{vertical}' AS tipo,
                                fo.user.id,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3

                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'

                                --No compraron en la vertical en los últimos 12 meses
                                AND fo.user.id NOT IN (SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	
                                WHERE fo.registered_date BETWEEN '{from_date_new_users}' AND '{to_date_lm}' AND fo.country_id = 3 
                                AND fo.order_status = 'CONFIRMED'
                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'


                                GROUP BY 1)
                                AND fo.user.id NOT IN(SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` du ON fo.user.id = du.user_id
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id
                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.data.firstSuccessful
                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'

                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                GROUP BY 1)	

                                GROUP BY 1,2	


                                ),

                acquisitions_tm as(	
                                SELECT
                                fo.user.id as user_id,	
                                '{vertical}' AS tipo,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	

                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                AND fo.data.firstSuccessful	

                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'

                                GROUP BY 1,2	

                                ),



                reactivated_tm_qc as(    

                            SELECT
                            DISTINCT fo.user.id

                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id 
                            LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id	
                            LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id

                            WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')    
                            AND fo.country_id = 3    
                            AND fo.order_status = 'CONFIRMED'    

                            AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                            AND dp.business_type.business_type_name = '{vertical}'


                            AND fo.user.id NOT IN    
                            (SELECT    
                            fo.user.id AS user_id    
                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id    
                            WHERE fo.registered_date BETWEEN DATE('{from_date_lm}') AND DATE('{to_date_lm}')
                            AND fo.country_id = 3 
                            AND fo.order_status = 'CONFIRMED'    

                            AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                            AND dp.business_type.business_type_name = '{vertical}'


                            GROUP BY 1)    

                            AND acquisitions_tm.user_id is null
                            AND new_users_tm_qc.id is null   

                            GROUP BY 1    


                )

                SELECT	

                  FORMAT_DATETIME('%Y-%m',fo.registered_date) AS Month,
                  CASE WHEN ((UPPER(UPPER((coalesce( dp.businessCategory.name ,'Without Category')))) LIKE UPPER ('Supermercado%')) OR (UPPER(UPPER((coalesce( dp.businessCategory.name ,'Without Category')))) LIKE UPPER ('Minimercado%')) OR (UPPER(UPPER((coalesce( dp.businessCategory.name,'Without Category')))) = UPPER('Express'))) then 'Supermercados'
                  WHEN UPPER(dpb.business_type_name) = 'MARKET' AND NOT dp.is_darkstore AND NOT (((UPPER(UPPER((coalesce( dp.businessCategory.name ,'Without Category')))) LIKE UPPER ('Supermercado%')) OR (UPPER(UPPER((coalesce( dp.businessCategory.name ,'Without Category')))) LIKE UPPER ('Minimercado%')) OR (UPPER(UPPER((coalesce( dp.businessCategory.name,'Without Category')))) = UPPER('Express')))) then 'Comercios Especializados'
                  END as vertical,                    
                  CASE WHEN reactivated_tm_qc.id is not null then 'Reactivados'
                  WHEN acquisitions_tm.user_id is not null then 'Acquisition'
                  WHEN new_users_tm_qc.id is not null then 'New Users'
                  END AS user_type,


                  --COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND p.fromDate IS NOT NULL AND p.toDate IS NOT NULL THEN fo.user.id END) AS q_orders_campana,

                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.has_voucher_discount > 0 THEN fo.user.id END) AS q_user_voucher,
                  count(distinct case when fo.order_status ='CONFIRMED' and (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) then fo.user.id end ) as Vouchers_CRM,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true THEN fo.user.id END) AS q_user_df0,
                  (COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') THEN fo.user.id END)) AS q_user_corporate,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND c.name IS NOT NULL THEN fo.user.id END) AS q_user_banks,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.is_user_plus = 1 THEN fo.user.id END) AS q_user_plus,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.user.id END) AS q_user_totales,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.order_id END) AS q_orders_totales,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.user.id END) AS q_users_incentive,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.order_id END) AS q_orders_incentive,

                  FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                    LEFT JOIN reactivated_tm_qc ON fo.user.id = reactivated_tm_qc.id	
                  LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id	
                  LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` dp ON dp.partner_id = fo.restaurant.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS dim_city__partner ON dp.city_id = dim_city__partner.city_id
                  LEFT JOIN UNNEST([dp.franchise]) as dp_franchise
                  LEFT JOIN UNNEST (fo.details) as fo_details
                  LEFT JOIN UNNEST (fo.discounts) as fo_discounts
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_product` AS dim_product ON fo_details.product.product_id = dim_product.product_id
                  LEFT JOIN `peya-data-origins-pro.cl_core.product` p ON dim_product.product_id = p.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` dsp ON fo_details.product.product_id = dsp.product_id AND fo_details.is_subsidized = true
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` dsc ON dsp.subsidized_product_campaing_id = dsc.subsidized_campaign_id
                  LEFT JOIN `peya-bi-tools-pro.il_wallet.fact_wallet` fw ON fo.order_id = fw.order_id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` ftc ON (fo.order_id = ftc.order_id AND DATE(ftc.coupon_created_at) > DATE_ADD(CURRENT_DATE(), INTERVAL -100 DAY))
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` dtc ON ftc.talon_campaign_id = dtc.talon_campaign_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.order_log` ol ON fo.order_id = ol.order_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign_log_items` cli ON ol.id = cli.order_log_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign` c ON cli.id = c.id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id
                  LEFT JOIN UNNEST([dp.business_type]) as dpb

                WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')

                AND fo.country_id = 3	
                AND (reactivated_tm_qc.id IS NOT NULL	OR acquisitions_tm.user_id IS NOT NULL OR new_users_tm_qc.id is not null)

                GROUP BY 1,2,3
                HAVING vertical is not null
                '''
            elif vertical == 'Local Stores':
                query = f'''
                
                with new_users_tm_qc as(	

                                SELECT
                                '{vertical}' AS tipo,
                                fo.user.id,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3

                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')

                                --No compraron en la vertical en los últimos 12 meses
                                AND fo.user.id NOT IN (SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	
                                WHERE fo.registered_date BETWEEN '{from_date_new_users}' AND '{to_date_lm}' AND fo.country_id = 3 
                                AND fo.order_status = 'CONFIRMED'
                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')


                                GROUP BY 1)
                                AND fo.user.id NOT IN(SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` du ON fo.user.id = du.user_id
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id
                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.data.firstSuccessful
                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')

                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                GROUP BY 1)	

                                GROUP BY 1,2	


                                ),

                acquisitions_tm as(	
                                SELECT
                                fo.user.id as user_id,	
                                '{vertical}' AS tipo,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	

                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                AND fo.data.firstSuccessful	

                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')

                                GROUP BY 1,2	

                                ),



                reactivated_tm_qc as(    

                            SELECT
                            DISTINCT fo.user.id

                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id 
                            LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id	
                            LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id

                            WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')    
                            AND fo.country_id = 3    
                            AND fo.order_status = 'CONFIRMED'    

                            AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                            AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')


                            AND fo.user.id NOT IN    
                            (SELECT    
                            fo.user.id AS user_id    
                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id    
                            WHERE fo.registered_date BETWEEN DATE('{from_date_lm}') AND DATE('{to_date_lm}')
                            AND fo.country_id = 3 
                            AND fo.order_status = 'CONFIRMED'    

                            AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                            AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')


                            GROUP BY 1)    

                            AND acquisitions_tm.user_id is null
                            AND new_users_tm_qc.id is null   

                            GROUP BY 1    


                )

                SELECT	

                  FORMAT_DATETIME('%Y-%m',fo.registered_date) AS Month,
                  '{vertical}' as vertical,                    
                  CASE WHEN reactivated_tm_qc.id is not null then 'Reactivados'
                  WHEN acquisitions_tm.user_id is not null then 'Acquisition'
                  WHEN new_users_tm_qc.id is not null then 'New Users'
                  END AS user_type,


                  --COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND p.fromDate IS NOT NULL AND p.toDate IS NOT NULL THEN fo.user.id END) AS q_orders_campana,

                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.has_voucher_discount > 0 THEN fo.user.id END) AS q_user_voucher,
                  count(distinct case when fo.order_status ='CONFIRMED' and (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) then fo.user.id end ) as Vouchers_CRM,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true THEN fo.user.id END) AS q_user_df0,
                  (COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') THEN fo.user.id END)) AS q_user_corporate,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND c.name IS NOT NULL THEN fo.user.id END) AS q_user_banks,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.is_user_plus = 1 THEN fo.user.id END) AS q_user_plus,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.user.id END) AS q_user_totales,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.order_id END) AS q_orders_totales,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.user.id END) AS q_users_incentive,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.order_id END) AS q_orders_incentive,

                  FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                    LEFT JOIN reactivated_tm_qc ON fo.user.id = reactivated_tm_qc.id	
                  LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id	
                  LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` dp ON dp.partner_id = fo.restaurant.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS dim_city__partner ON dp.city_id = dim_city__partner.city_id
                  LEFT JOIN UNNEST([dp.franchise]) as dp_franchise
                  LEFT JOIN UNNEST (fo.details) as fo_details
                  LEFT JOIN UNNEST (fo.discounts) as fo_discounts
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_product` AS dim_product ON fo_details.product.product_id = dim_product.product_id
                  LEFT JOIN `peya-data-origins-pro.cl_core.product` p ON dim_product.product_id = p.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` dsp ON fo_details.product.product_id = dsp.product_id AND fo_details.is_subsidized = true
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` dsc ON dsp.subsidized_product_campaing_id = dsc.subsidized_campaign_id
                  LEFT JOIN `peya-bi-tools-pro.il_wallet.fact_wallet` fw ON fo.order_id = fw.order_id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` ftc ON (fo.order_id = ftc.order_id AND DATE(ftc.coupon_created_at) > DATE_ADD(CURRENT_DATE(), INTERVAL -100 DAY))
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` dtc ON ftc.talon_campaign_id = dtc.talon_campaign_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.order_log` ol ON fo.order_id = ol.order_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign_log_items` cli ON ol.id = cli.order_log_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign` c ON cli.id = c.id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id
                  LEFT JOIN UNNEST([dp.business_type]) as dpb

                WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')

                AND fo.country_id = 3	
                AND (reactivated_tm_qc.id IS NOT NULL	OR acquisitions_tm.user_id IS NOT NULL OR new_users_tm_qc.id is not null)

                GROUP BY 1,2,3
                '''
            elif vertical == 'QC':
                query = f'''
                
                with new_users_tm_qc as(	

                                SELECT
                                '{vertical}' AS tipo,
                                fo.user.id,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3

                                 
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')

                                --No compraron en la vertical en los últimos 12 meses
                                AND fo.user.id NOT IN (SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	
                                WHERE fo.registered_date BETWEEN '{from_date_new_users}' AND '{to_date_lm}' AND fo.country_id = 3 
                                AND fo.order_status = 'CONFIRMED'
                                 
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')


                                GROUP BY 1)
                                AND fo.user.id NOT IN(SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` du ON fo.user.id = du.user_id
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id
                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.data.firstSuccessful
                                 
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')

                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                GROUP BY 1)	

                                GROUP BY 1,2	


                                ),

                acquisitions_tm as(	
                                SELECT
                                fo.user.id as user_id,	
                                '{vertical}' AS tipo,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	

                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                AND fo.data.firstSuccessful	

                                 
                                AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')

                                GROUP BY 1,2	

                                ),



                reactivated_tm_qc as(    

                            SELECT
                            DISTINCT fo.user.id

                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id 
                            LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id	
                            LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id

                            WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')    
                            AND fo.country_id = 3    
                            AND fo.order_status = 'CONFIRMED'    

                             
                            AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')


                            AND fo.user.id NOT IN    
                            (SELECT    
                            fo.user.id AS user_id    
                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id    
                            WHERE fo.registered_date BETWEEN DATE('{from_date_lm}') AND DATE('{to_date_lm}')
                            AND fo.country_id = 3 
                            AND fo.order_status = 'CONFIRMED'    

                             
                            AND dp.business_type.business_type_name NOT IN ('Restaurant','Coffee')


                            GROUP BY 1)    

                            AND acquisitions_tm.user_id is null
                            AND new_users_tm_qc.id is null   

                            GROUP BY 1    


                )

                SELECT	

                  FORMAT_DATETIME('%Y-%m',fo.registered_date) AS Month,
                  '{vertical}' as vertical,                    
                  CASE WHEN reactivated_tm_qc.id is not null then 'Reactivados'
                  WHEN acquisitions_tm.user_id is not null then 'Acquisition'
                  WHEN new_users_tm_qc.id is not null then 'New Users'
                  END AS user_type,


                  --COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND p.fromDate IS NOT NULL AND p.toDate IS NOT NULL THEN fo.user.id END) AS q_orders_campana,

                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.has_voucher_discount > 0 THEN fo.user.id END) AS q_user_voucher,
                  count(distinct case when fo.order_status ='CONFIRMED' and (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) then fo.user.id end ) as Vouchers_CRM,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true THEN fo.user.id END) AS q_user_df0,
                  (COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') THEN fo.user.id END)) AS q_user_corporate,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND c.name IS NOT NULL THEN fo.user.id END) AS q_user_banks,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.is_user_plus = 1 THEN fo.user.id END) AS q_user_plus,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.user.id END) AS q_user_totales,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.order_id END) AS q_orders_totales,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.user.id END) AS q_users_incentive,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.order_id END) AS q_orders_incentive,

                  FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                    LEFT JOIN reactivated_tm_qc ON fo.user.id = reactivated_tm_qc.id	
                  LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id	
                  LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` dp ON dp.partner_id = fo.restaurant.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS dim_city__partner ON dp.city_id = dim_city__partner.city_id
                  LEFT JOIN UNNEST([dp.franchise]) as dp_franchise
                  LEFT JOIN UNNEST (fo.details) as fo_details
                  LEFT JOIN UNNEST (fo.discounts) as fo_discounts
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_product` AS dim_product ON fo_details.product.product_id = dim_product.product_id
                  LEFT JOIN `peya-data-origins-pro.cl_core.product` p ON dim_product.product_id = p.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` dsp ON fo_details.product.product_id = dsp.product_id AND fo_details.is_subsidized = true
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` dsc ON dsp.subsidized_product_campaing_id = dsc.subsidized_campaign_id
                  LEFT JOIN `peya-bi-tools-pro.il_wallet.fact_wallet` fw ON fo.order_id = fw.order_id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` ftc ON (fo.order_id = ftc.order_id AND DATE(ftc.coupon_created_at) > DATE_ADD(CURRENT_DATE(), INTERVAL -100 DAY))
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` dtc ON ftc.talon_campaign_id = dtc.talon_campaign_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.order_log` ol ON fo.order_id = ol.order_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign_log_items` cli ON ol.id = cli.order_log_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign` c ON cli.id = c.id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id
                  LEFT JOIN UNNEST([dp.business_type]) as dpb

                WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')

                AND fo.country_id = 3	
                AND (reactivated_tm_qc.id IS NOT NULL	OR acquisitions_tm.user_id IS NOT NULL OR new_users_tm_qc.id is not null)

                GROUP BY 1,2,3
                '''



                
            else:  

            # Escribimos la query
                query = f'''
                with new_users_tm_qc as(	

                                SELECT
                                '{vertical}' AS tipo,
                                fo.user.id,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3

                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'

                                --No compraron en la vertical en los últimos 12 meses
                                AND fo.user.id NOT IN (SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	
                                WHERE fo.registered_date BETWEEN '{from_date_new_users}' AND '{to_date_lm}' AND fo.country_id = 3 
                                AND fo.order_status = 'CONFIRMED'
                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'


                                GROUP BY 1)
                                AND fo.user.id NOT IN(SELECT
                                fo.user.id AS user_id
                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_user` du ON fo.user.id = du.user_id
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id
                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.data.firstSuccessful
                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'

                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                GROUP BY 1)	

                                GROUP BY 1,2	


                                ),

                acquisitions_tm as(	
                                SELECT
                                fo.user.id as user_id,	
                                '{vertical}' AS tipo,
                                COUNT(DISTINCT case when fo.order_status = 'CONFIRMED' then fo.user.id else NULL end ) AS users	

                                FROM `peya-bi-tools-pro.il_core.fact_orders` fo	
                                LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id	
                                LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id	
                                LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id	

                                WHERE fo.registered_date BETWEEN '{from_date}' AND '{to_date}'
                                AND fo.country_id = 3 AND fo.order_status = 'CONFIRMED'	
                                AND fo.data.firstSuccessful	

                                AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                                AND dp.business_type.business_type_name = '{vertical}'

                                GROUP BY 1,2	

                                ),



                reactivated_tm_qc as(    

                            SELECT
                            DISTINCT fo.user.id

                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id 
                            LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id	
                            LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id

                            WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')    
                            AND fo.country_id = 3    
                            AND fo.order_status = 'CONFIRMED'    

                            AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                            AND dp.business_type.business_type_name = '{vertical}'


                            AND fo.user.id NOT IN    
                            (SELECT    
                            fo.user.id AS user_id    
                            FROM `peya-bi-tools-pro.il_core.fact_orders` fo    
                            LEFT JOIN `peya-markets.automated_tables_reports.regiones_oficinas_kam` AS r ON fo.restaurant.id = r.Id    
                            LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` AS dp ON dp.partner_id= fo.restaurant.id    
                            WHERE fo.registered_date BETWEEN DATE('{from_date_lm}') AND DATE('{to_date_lm}')
                            AND fo.country_id = 3 
                            AND fo.order_status = 'CONFIRMED'    

                            AND (dp.is_darkstore = FALSE OR dp.is_darkstore IS NULL)
                            AND dp.business_type.business_type_name = '{vertical}'


                            GROUP BY 1)    

                            AND acquisitions_tm.user_id is null
                            AND new_users_tm_qc.id is null   

                            GROUP BY 1    


                )

                SELECT	

                  FORMAT_DATETIME('%Y-%m',fo.registered_date) AS Month,
                  '{vertical}' AS vertical,
                    CASE WHEN reactivated_tm_qc.id is not null then 'Reactivados'
                  WHEN acquisitions_tm.user_id is not null then 'Acquisition'
                  WHEN new_users_tm_qc.id is not null then 'New Users'
                  END AS user_type,


                  --COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND p.fromDate IS NOT NULL AND p.toDate IS NOT NULL THEN fo.user.id END) AS q_orders_campana,

                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.has_voucher_discount > 0 THEN fo.user.id END) AS q_user_voucher,
                  count(distinct case when fo.order_status ='CONFIRMED' and (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) then fo.user.id end ) as Vouchers_CRM,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true THEN fo.user.id END) AS q_user_df0,
                  (COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') THEN fo.user.id END)) AS q_user_corporate,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND c.name IS NOT NULL THEN fo.user.id END) AS q_user_banks,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND fo.is_user_plus = 1 THEN fo.user.id END) AS q_user_plus,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.user.id END) AS q_user_totales,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' THEN fo.order_id END) AS q_orders_totales,
                  COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.user.id END) AS q_users_incentive,
                    COUNT(DISTINCT CASE WHEN fo.order_status = 'CONFIRMED' AND (fo.has_voucher_discount > 0) OR (dtc.talon_campaign_name LIKE ('%LOCAL%') OR dtc.talon_campaign_name LIKE ('%CENTRAL%')) OR (fo.shipping_amount_no_discount = 0 AND fo.with_logistics = true) OR (dtc.talon_campaign_name LIKE '%COOP%' OR fw.campaign_name LIKE '%COOP%') OR c.name IS NOT NULL OR fo.is_user_plus = 1 THEN fo.order_id END) AS q_orders_incentive,

                  FROM `peya-bi-tools-pro.il_core.fact_orders` fo
                    LEFT JOIN reactivated_tm_qc ON fo.user.id = reactivated_tm_qc.id	
                  LEFT JOIN acquisitions_tm ON fo.user.id = acquisitions_tm.user_id	
                  LEFT JOIN new_users_tm_qc ON new_users_tm_qc.id = fo.user.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_partner` dp ON dp.partner_id = fo.restaurant.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_city` AS dim_city__partner ON dp.city_id = dim_city__partner.city_id
                  LEFT JOIN UNNEST([dp.franchise]) as dp_franchise
                  LEFT JOIN UNNEST (fo.details) as fo_details
                  LEFT JOIN UNNEST (fo.discounts) as fo_discounts
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_product` AS dim_product ON fo_details.product.product_id = dim_product.product_id
                  LEFT JOIN `peya-data-origins-pro.cl_core.product` p ON dim_product.product_id = p.id
                  LEFT JOIN `peya-bi-tools-pro.il_core.dim_subsidized_product` dsp ON fo_details.product.product_id = dsp.product_id AND fo_details.is_subsidized = true
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_subsidized_campaign` dsc ON dsp.subsidized_product_campaing_id = dsc.subsidized_campaign_id
                  LEFT JOIN `peya-bi-tools-pro.il_wallet.fact_wallet` fw ON fo.order_id = fw.order_id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.fact_talon_coupons` ftc ON (fo.order_id = ftc.order_id AND DATE(ftc.coupon_created_at) > DATE_ADD(CURRENT_DATE(), INTERVAL -100 DAY))
                  LEFT JOIN `peya-bi-tools-pro.il_growth.dim_talon_campaigns` dtc ON ftc.talon_campaign_id = dtc.talon_campaign_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.order_log` ol ON fo.order_id = ol.order_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign_log_items` cli ON ol.id = cli.order_log_id
                  LEFT JOIN `peya-data-origins-pro.cl_campaigns.campaign` c ON cli.id = c.id
                  LEFT JOIN `peya-bi-tools-pro.il_growth.agg_user_lifecycle` aul ON fo.user.id = aul.user_id

                WHERE fo.registered_date BETWEEN DATE('{from_date}') AND DATE('{to_date}')

                AND fo.country_id = 3	
                AND (reactivated_tm_qc.id IS NOT NULL	OR acquisitions_tm.user_id IS NOT NULL OR new_users_tm_qc.id is not null)

                GROUP BY 1,2,3'''

                # Descargar la data
            data = pd.io.gbq.read_gbq(query, project_id='peya-argentina', dialect='standard')

            # Si m_orders está vacío, le asignamos el valor de data
            if df.empty:
                df = data
            else:
                # Concatenamos el DataFrame existente con el nuevo DataFrame
                df = pd.concat([df, data], ignore_index=True)




# In[38]:


df


# In[ ]:


pd.set_option('display.max_rows', None)
df


# In[39]:


df.to_csv('QC_user_evolution.csv', index=False)


# In[ ]:




