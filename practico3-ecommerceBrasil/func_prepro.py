import numpy as np
import pandas as pd

def preprocesa(path):
    """
    Funcion de preprocesamiento de los datos, recibe un path para cargar un csv y lo preprocesa
    """

    all_data = pd.read_csv(r''+path)
    
    
    #MANEJO DE VARIABLES

    # Cambie el tipo de datos en la columna de fecha para que el tipo de datos finalice
    date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date',
             'order_estimated_delivery_date', 'shipping_limit_date', 'review_creation_date', 'review_answer_timestamp'] 
    for col in date_columns:
        all_data[col] = pd.to_datetime(all_data[col], format='%Y-%m-%d %H:%M:%S')
    
    #Cree una columna de mes_orden para la exploración de datos
    all_data['Month_order'] = all_data['order_purchase_timestamp'].dt.to_period('M').astype('str')
        
    # Elija entradas que van desde 01-2017 hasta 08-2018
    #Porque hay datos que están fuera de balance con el promedio de cada mes en los datos antes del 01-2017 y después del 08-2018
    # basado en datos de compra / order_purchase_timestamp
    start_date = "2017-01-01"
    end_date = "2018-08-31"

    after_start_date = all_data['order_purchase_timestamp'] >= start_date
    before_end_date = all_data['order_purchase_timestamp'] <= end_date
    between_two_dates = after_start_date & before_end_date
    all_data = all_data.loc[between_two_dates]
    
    
    #MANEJO DE VALORES PERDIDOS
    
    # Gestiona entradas vacías en la columna order_approved_at
    missing_1 = all_data['order_approved_at'] - all_data['order_purchase_timestamp']
    
    # tomamos la mediana porque hay quienes aprueban directamente desde el momento en que ordena, algunos son de hasta 60 días
    add_1 = all_data[all_data['order_approved_at'].isnull()]['order_purchase_timestamp'] + missing_1.median()
    all_data['order_approved_at']= all_data['order_approved_at'].replace(np.nan, add_1)
    
    # Gestión de entradas vacías en la columna order_delivered_carrier_date
    missing_2 = all_data['order_delivered_carrier_date'] - all_data['order_approved_at']
   
    # Tomamos la mediana porque algunos barcos están dentro de las 21 horas del tiempo acordado, algunos hasta 107 días
    add_2 = all_data[all_data['order_delivered_carrier_date'].isnull()]['order_approved_at'] + missing_2.median()
    all_data['order_delivered_carrier_date']= all_data['order_delivered_carrier_date'].replace(np.nan, add_2)
    
    # Gestión de entradas vacías en la columna order_delivered_customer_date
    missing_3 = all_data['order_delivered_customer_date'] - all_data['order_delivered_carrier_date']
    
    # tomamos la mediana porque hay un tiempo de entrega de -17 días, lo que significa que es atípico, también hay un tiempo de entrega de hasta 205 días
    add_3 = all_data[all_data['order_delivered_customer_date'].isnull()]['order_delivered_carrier_date'] + missing_3.median()
    all_data['order_delivered_customer_date']= all_data['order_delivered_customer_date'].replace(np.nan, add_3)
    
    # Manejar las columnas review_comment_title y review_comment_message
    #Porque el número de entradas en blanco es muy grande e imposible de completar porque no hay variables que puedan
    # usado para calcularlo. Porque este es el comentario y el título del comentario
    # Luego eliminaremos la columna
    all_data = all_data.drop(['review_comment_title', 'review_comment_message'], axis=1)
    
    # Entrega de entrada vacía en las columnas product_weight_g, product_length_cm, product_height_cm, product_width_cm
    #Porque solo hay 1, entonces lo dejamos caer
    all_data = all_data.dropna()

    # Ajuste el tipo de datos con los datos de entrada
    all_data = all_data.astype({'order_item_id': 'int64', 
                            'product_name_lenght': 'int64',
                            'product_description_lenght':'int64', 
                            'product_photos_qty':'int64'})
    
    
    #EXTRACCIÓN DE CARACTERÍSTICAS
    
    #Cree una columna order_process_time para ver cuánto tiempo llevará iniciar el pedido hasta que los
    # artículos son aceptados por los clientes
    all_data['order_process_time'] = all_data['order_delivered_customer_date'] - all_data['order_purchase_timestamp']
    
    #Cree una columna order_process_time para ver cuánto tiempo llevará iniciar el pedido hasta que los
    # artículos son aceptados por los clientes
    all_data['order_process_time'] = all_data['order_delivered_customer_date'] - all_data['order_purchase_timestamp']

    #Cree una columna order_delivery_time para ver cuánto tiempo requiere el tiempo de envío para cada pedido (desde que se envia
    #hasta que llega a manos del cliente)
    all_data['order_delivery_time'] = all_data['order_delivered_customer_date'] - all_data['order_delivered_carrier_date']

    #Cree una columna order_time_accuracy para ver si desde el tiempo estimado hasta el tiempo en que llego, fue
    #algo apropiado o llego tarde
    # Si el valor es + positivo, entonces es más rápido, si es 0, está justo a tiempo, pero si es negativo, llego tarde
    all_data['order_accuracy_time'] = all_data['order_estimated_delivery_date'] - all_data['order_delivered_customer_date'] 

    #Cree una columna order_approved_time para ver cuánto tiempo tomará desde el pedido hasta la aprobación
    all_data['order_approved_time'] = all_data['order_approved_at'] - all_data['order_purchase_timestamp'] 

    #Cree una columna review_send_time para averiguar cuánto tiempo tardo en enviarse la encuesta de satisfacción después de
    #recibir el artículo.
    all_data['review_send_time'] = all_data['review_creation_date'] - all_data['order_delivered_customer_date']

    #Cree una columna review_answer_time para averiguar cuánto tiempo llevará completar una revisión de respuesta después de
    # enviar una encuesta de satisfacción del cliente.
    all_data['review_answer_time'] = all_data['review_answer_timestamp'] - all_data['review_creation_date']

    # Combine las columnas product_length_cm, product_height_cm y product_width_cm para convertirlo en un volumen
    # con una nueva columna, volumen_producto
    all_data['product_volume'] = all_data['product_length_cm'] * all_data['product_height_cm'] * all_data['product_width_cm']
    
    

    return all_data
