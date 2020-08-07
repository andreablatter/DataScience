import numpy as np
import pandas as pd
from datetime import datetime

def preprocesa(df):
    """
    Funcion de preprocesamiento de los datos, recibe un path para cargar un csv y lo preprocesa
    """

    #df = pd.read_csv(r''+path)
    
    #df = df.drop(['Unnamed: 0'], axis=1)
    print (df)
    #df.to_datetime(data['order_purchase_timestamp'])
   
    # Cambie el tipo de datos en la columna de fecha para que el tipo de datos finalice
    #date_columns = ['order_purchase_timestamp']
    date_columns = ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date',
                 'order_estimated_delivery_date', 'shipping_limit_date', 'review_creation_date', 'review_answer_timestamp']
    
    for col in date_columns:
       df[col] = pd.to_datetime(df[date_columns], format='%Y-%m-%d %H:%M:%S', yearfirst=True, errors='coerce')

    #, yearfirst=True, errors='coerce'

    
    #Cree una columna de mes_orden para la exploración de datos
    df['Month_order'] = df['order_purchase_timestamp'].dt.to_period('M').astype('str')

    
    # Elija entradas que van desde 01-2017 hasta 08-2018
    #Porque hay datos que están fuera de balance con el promedio de cada mes en los datos antes del 01-2017 y después del 08-2018
    # basado en datos de compra / order_purchase_timestamp
    start_date = "2017-01-01"
    end_date = "2018-08-31"

    after_start_date = df['order_purchase_timestamp'] >= start_date
    before_end_date = df['order_purchase_timestamp'] <= end_date
    between_two_dates = after_start_date & before_end_date
    df = df.loc[between_two_dates]


    # Compartir datos según el tipo de datos
    only_numeric = df.select_dtypes(include=['int', 'float'])
    only_object = df.select_dtypes(include=['object'])
    only_time = df.select_dtypes(include=['datetime', 'timedelta'])

    # Gestiona entradas vacías en la columna order_approved_at
    missing_1 = df['order_approved_at'] - df['order_purchase_timestamp']
    print(missing_1.describe())
    print('='*50)
    print('Mediana desde el momento en que se aprobó la orden: ',missing_1.median())

    # tomamos la mediana porque hay quienes aprueban directamente desde el momento en que ordena, algunos son de hasta 60 días
    add_1 = df[df['order_approved_at'].isnull()]['order_purchase_timestamp'] + missing_1.median()
    df['order_approved_at']= df['order_approved_at'].replace(np.nan, add_1)

    # Gestión de entradas vacías en la columna order_delivered_carrier_date
    missing_2 = df['order_delivered_carrier_date'] - df['order_approved_at']
    print(missing_2.describe())
    print('='*50)
    print('Mediana desde el momento de la solicitud hasta el envío: ',missing_2.median())

    # Tomamos la mediana porque algunos barcos están dentro de las 21 horas del tiempo acordado, algunos hasta 107 días
    add_2 = df[df['order_delivered_carrier_date'].isnull()]['order_approved_at'] + missing_2.median()
    df['order_delivered_carrier_date']= df['order_delivered_carrier_date'].replace(np.nan, add_2)

    # Gestión de entradas vacías en la columna order_delivered_customer_date
    missing_3 = df['order_delivered_customer_date'] - df['order_delivered_carrier_date']
    print(missing_3.describe())
    print('='*50)
    print('Mediana desde el momento en que se envió hasta que el cliente la recibió: ',missing_3.median())

    # tomamos la mediana porque hay un tiempo de entrega de -17 días, lo que significa que es atípico, también hay un tiempo de entrega de hasta 205 días
    add_3 = df[df['order_delivered_customer_date'].isnull()]['order_delivered_carrier_date'] + missing_3.median()
    df['order_delivered_customer_date']= df['order_delivered_customer_date'].replace(np.nan, add_3)

    # Manejar las columnas review_comment_title y review_comment_message
    #Porque el número de entradas en blanco es muy grande e imposible de completar porque no hay variables que puedan
    # usado para calcularlo. Porque este es el comentario y el título del comentario
    # Luego eliminaremos la columna
    df = df.drop(['review_comment_title', 'review_comment_message'], axis=1)

    # Entrega de entrada vacía en las columnas product_weight_g, product_length_cm, product_height_cm, product_width_cm
    #Porque solo hay 1, entonces lo dejamos caer
    df = df.dropna()

    # Ajuste el tipo de datos con los datos de entrada
    df = df.astype({'order_item_id': 'int64',
                                'product_name_lenght': 'int64',
                                'product_description_lenght':'int64',
                                'product_photos_qty':'int64'})
     # Extraccion de CARACTERISTICAS
    #Cree una columna order_process_time para ver cuánto tiempo llevará iniciar el pedido hasta
    # artículos son aceptados por los clientes
    df['order_process_time'] = df['order_delivered_customer_date'] - df['order_purchase_timestamp']

    #Cree una columna order_delivery_time para ver cuánto tiempo se requiere el tiempo de envío para cada pedido
    df['order_delivery_time'] = df['order_delivered_customer_date'] - df['order_delivered_carrier_date']

    #Cree una columna order_time_accuracy para ver si desde el tiempo estimado hasta que algo sea apropiado o tarde
    # Si el valor es + positivo, entonces es más rápido hasta que, si es 0, está justo a tiempo, pero si es negativo, llega tarde
    df['order_accuracy_time'] = df['order_estimated_delivery_date'] - df['order_delivered_customer_date']

    #Cree una columna order_approved_time para ver cuánto tiempo tomará desde el pedido hasta la aprobación
    df['order_approved_time'] = df['order_approved_at'] - df['order_purchase_timestamp']

    #Cree una columna review_send_time para averiguar cuánto tiempo se envió la encuesta de satisfacción después de recibir el artículo.
    df['review_send_time'] = df['review_creation_date'] - df['order_delivered_customer_date']

    #Cree una columna review_answer_time para averiguar cuánto tiempo llevará completar una revisión después de
    # envió una encuesta de satisfacción del cliente.
    df['review_answer_time'] = df['review_answer_timestamp'] - df['review_creation_date']

    # Combine las columnas product_length_cm, product_height_cm y product_width_cm para convertirlo en un volumen
    # con una nueva columna, volumen_producto
    df['product_volume'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']
    
    return df
