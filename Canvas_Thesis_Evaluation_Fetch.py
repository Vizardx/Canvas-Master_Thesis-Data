import pandas as pd
import requests
import os
import urllib.request
from concurrent.futures import ThreadPoolExecutor
import os
import adal
import config
import aiohttp
import asyncio
import schedule
import time
import numpy as np
from pandas import json_normalize
import shutil

def run_asyncio_job(job):
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    new_loop.run_until_complete(job())
    new_loop.close()


async def job():
    async with aiohttp.ClientSession() as session:
        TFM_id = 165
        Directorio = "Downloads"
        Resultados = f"{Directorio}/Final_Files"


        BASE_URL = 'https://yourcompany.instructure.com' # Set the correct URL for your company, like https://oxford.instructure.com'
        ACCESS_TOKEN = 'x'
        # headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}


        async def get_tfm_courses(tfm):
                    """
                    Retrieves the courses for a given TFM account.

                    Args:
                        tfm (str): The TFM account.

                    Returns:
                        pandas.DataFrame: A DataFrame containing the TFM courses.
                    """
                    print(f"Recuperando Cursos para Account {tfm} . . .")
                    url = f'{BASE_URL}/api/v1/accounts/{tfm}/courses'
                    params = {'per_page': 100}
                    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
                    all_courses = []
                    async with aiohttp.ClientSession() as session:
                        while url:
                            async with session.get(url, headers=headers, params=params) as response:
                                data = await response.json()
                                all_courses.extend(data)
                                url = response.links.get('next', {}).get('url')
                    all_courses_df = pd.DataFrame(all_courses)
                    all_courses_df.rename(columns={'id': 'course_id'}, inplace=True)
                    # Cogemos solo GT-2023, GT-2024 y ES-2024
                    tfm_courses_df = all_courses_df[(all_courses_df['enrollment_term_id'] == 237) | (all_courses_df['enrollment_term_id'] == 238)| (all_courses_df['enrollment_term_id'] == 241)].copy()
                    tfm_courses_df['year'] = tfm_courses_df['enrollment_term_id'].replace({241: 'GT-2023', 238: 'GT-2024', 237: 'ES-2024'})
                    return tfm_courses_df

        async def get_course_sections(course_id, include=None):
                    """
                    Retrieves the sections of a course.

                    Parameters:
                    - course_id (int): The ID of the course.
                    - include (list, optional): Additional data to include in the response. Defaults to None.

                    Returns:
                    - pandas.DataFrame: A DataFrame containing the sections of the course.
                    """
                    if include is None:
                        include = []
                    url = f'{BASE_URL}/api/v1/courses/{course_id}/sections'
                    params = {'include[]': include, 'per_page': 100}
                    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
                    sections = []
                    async with aiohttp.ClientSession() as session:
                        while url:
                            async with session.get(url, headers=headers, params=params) as response:
                                data = await response.json()
                                sections.extend(data)
                                url = response.links.get('next', {}).get('url')
                    return pd.DataFrame(sections)


        async def get_tutors(course_id):
            url = f'{BASE_URL}/api/v1/courses/{course_id}/users'
            params = {'enrollment_role': 'Tutor', 'per_page': 100}
            headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
            tutors = []
            async with aiohttp.ClientSession() as session:
                while url:
                    async with session.get(url, headers=headers, params=params) as response:
                        data = await response.json()
                        tutors.extend(data)
                        url = response.links.get('next', {}).get('url')
            return pd.DataFrame(tutors)


        async def get_students(course_id):
            url = f'{BASE_URL}/api/v1/courses/{course_id}/users'
            params = {'enrollment_type[]': 'student', 'include[]': 'avatar_url', 'per_page': 100}
            headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
            students = []
            async with aiohttp.ClientSession() as session:
                while url:
                    async with session.get(url, headers=headers, params=params) as response:
                        data = await response.json()
                        for student in data:
                            student['course_id'] = course_id
                        students.extend(data)
                        url = response.links.get('next', {}).get('url')
            return pd.DataFrame(students)


        def get_course_code(df_secciones, df_cursos):
            df_reducido = df_cursos[['course_id','course_code','name']].copy()
            df_reducido.rename(columns={'name': 'course_name'}, inplace=True)
            df_secciones = df_secciones.join(df_reducido.set_index('course_id'), on='course_id')
            return df_secciones

        def expand_sections(df):
            print("EXPANDIENDO SECCIONES . . .")
            # Define las columnas que quieres extraer
            columns = ['id', 'sortable_name', 'avatar_url']
            #df['students'] = df['students'].apply(literal_eval)

            # Agrega el prefijo a los nombres de las columnas
            prefixed_columns = []
            for i in range(1, 5):
                for column in columns:
                    prefixed_columns.append(f'student_{column}_{i}')

            # Crea un nuevo DataFrame vacío con las columnas especificadas
            new_df = pd.DataFrame(columns=prefixed_columns)

            # Itera sobre cada fila del DataFrame original
            for index, row in df.iterrows():
                # Obtiene los datos de la columna "students"
                data = row['students']
                # Inicializa un diccionario vacío para almacenar los valores extraídos
                extracted_values = {}
                # Itera sobre cada columna que quieres extraer
                for i, prefixed_column in enumerate(prefixed_columns):
                    # Calcula el índice del estudiante en la lista
                    student_index = i // len(columns)
                    # Verifica si hay suficientes estudiantes en la lista
                    if student_index < len(data):
                        student_data = data[student_index]
                        column = columns[i % len(columns)]
                        value = student_data[column].title() if column == 'sortable_name' and student_data[column] is not None else student_data[column]

                    else:
                        value = ''
                    # Almacena el valor en el diccionario con el nombre de columna con prefijo
                    extracted_values[prefixed_column] = value
                # Crea un nuevo DataFrame con una sola fila con los valores extraídos
                row_df = pd.DataFrame([extracted_values])
                # Concatena el nuevo DataFrame con el DataFrame original
                new_df = pd.concat([new_df, row_df], ignore_index=True)

            # Concatena el nuevo DataFrame con el DataFrame original a lo largo del eje de las columnas
            df.reset_index(drop=True, inplace=True)
            new_df.reset_index(drop=True, inplace=True)
            result = pd.concat([df, new_df], axis=1)

            # Guarda el nuevo DataFrame en un archivo CSV utilizando quoting=csv.QUOTE_NONNUMERIC
            return result


        def download_avatars(df, directory):
            """Descarga los avatares de los estudiantes en un DataFrame dado."""
            print("RECUPERANDO AVATARES . . .")
            # Crea la carpeta si no existe
            os.makedirs(directory, exist_ok=True)

            # Función para descargar una imagen
            def download_image(image_url, file_path):
                # Descarga la imagen y la guarda en la ruta especificada
                urllib.request.urlretrieve(image_url, file_path)

            # Crea un ThreadPoolExecutor para ejecutar las descargas en paralelo
            with ThreadPoolExecutor() as executor:
                # Itera sobre cada fila del DataFrame
                for index, row in df.iterrows():
                    # Obtiene la URL del avatar del estudiante
                    image_url = row['avatar_url']
                    
                    # Verifica si la URL del avatar no es nula
                    if pd.notnull(image_url):
                        # Construye el nombre del archivo con el formato especificado
                        student_id = int(row['id'])
                        filename = f"{student_id}.jpg"
                        
                        # Construye la ruta completa del archivo incluyendo la carpeta de imágenes
                        file_path = os.path.join(directory, filename)
                        
                        # Envía la tarea de descargar la imagen al executor
                        executor.submit(download_image, image_url, file_path)


        def get_headers_365(config):
            
            context = adal.AuthenticationContext(config.authority_url)
            token = context.acquire_token_with_client_credentials(config.resource_url, config.client_id, config.client_secret)
            headers_365 = {
                'Authorization': 'Bearer ' + token['accessToken'],
                'OData-MaxVersion': '4.0',
                'OData-Version': '4.0',
                'Accept': 'application/json',
                'Content-Type': 'application/json; charset=utf-8',
                'Prefer': 'odata.maxpagesize=500',
                'Prefer': 'odata.include-annotations=OData.Community.Display.V1.FormattedValue'
            }
            print("CONECTADO CON DYNAMICS!!")
            return headers_365

        def get_tutors_id_365(all_tutors_df, config, headers_365):
            
            def calculate_id(row):
                if pd.notnull(row['bit_dni']):
                    return row['bit_dni']
                elif pd.notnull(row['bit_nie']):
                    return row['bit_nie']
                elif pd.notnull(row['bit_numeropasaporte']):
                    return row['bit_numeropasaporte']
                elif pd.notnull(row['bit_otrodocumento']):
                    return row['bit_otrodocumento']
                elif pd.notnull(row['bit_cif']):
                    return row['bit_cif']
                else:
                    return None
            # Creamos una lista con todos los ids de los tutores
            tutor_ids = all_tutors_df['id'].tolist()

            # Convertimos la lista de ids en una cadena para usar en la consulta OData
            tutor_ids_profesor_str = ' or '.join(f"bit_canvasidprofesor eq '{id}'" for id in tutor_ids)
            tutor_ids_alumno_str = ' or '.join(f"bit_canvasidalumno eq '{id}'" for id in tutor_ids)

            # Realizamos la consulta OData
            query = f"/contacts?$filter=(statecode eq 0) and (({tutor_ids_profesor_str}) or ({tutor_ids_alumno_str}))&$select=contactid,bit_canvasidprofesor,bit_canvasidalumno,bit_dni,bit_numeropasaporte,bit_nie,bit_cif,bit_otrodocumento"
            response = requests.get(config.resource_url + '/api/data/v9.0' + query, headers=headers_365)

            # Convertimos la respuesta en un DataFrame
            contacts_df = pd.DataFrame(response.json()['value'])
            
            # Convertimos la columna 'id' de all_tutors_df a str
            all_tutors_df['id'] = all_tutors_df['id'].astype(str)
            contacts_df['bit_canvasidprofesor'] = contacts_df['bit_canvasidprofesor'].astype(str)
            # Ahora podemos combinar all_tutors_df con contacts_df basándonos en el id
            all_tutors_df = pd.merge(all_tutors_df, contacts_df, left_on='id', right_on='bit_canvasidprofesor', how='left')

            # Añadimos la columna id_calculado
            all_tutors_df['id_calculado'] = all_tutors_df.apply(calculate_id, axis=1)
            # Eliminamos los duplicados basándonos en la columna 'id'
            # all_tutors_df = all_tutors_df.drop_duplicates(subset='id')
            print("!!!DNIs RECUPERADOS!!!")
            
            return all_tutors_df
        
     
        '''def df_to_json(df, filename, orient='records'):
            # Convertir el DataFrame a JSON
            json_str = df.to_json(orient=orient)

            # Guardar la cadena JSON en un archivo con la codificación correcta
            with open(filename, 'w', encoding='utf-8') as file:
                file.write(json_str)
  
        async def get_section_tutor(section_id):
            url = f'{BASE_URL}/api/v1/sections/{section_id}/enrollments?type[]=TeacherEnrollment'
            headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    data = await response.json()
            return data[0]["user"]["id"] if data else None

        def get_tutor_id(section_id):
            loop1 = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(get_section_tutor(section_id))
            finally:
                loop.close()'''

        def get_section_tutor(section_id):
            print(f"Recuperando tutor de la sección {section_id}")
            url = f'{BASE_URL}/api/v1/sections/{section_id}/enrollments?type[]=TeacherEnrollment'
            headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
            response = requests.get(url, headers=headers)
            data = response.json()
            return data[0]["user"]["id"] if data else "Vacío"


        def get_course_parent(all_sections_df, config, headers_365):
            # Creamos una lista con todos los ids de los tutores
            courses_id = list(set(all_sections_df['course_id']))

            # Convertimos la lista de ids en una cadena para usar en la consulta OData
            courses_id_str = ' or '.join(f"bit_canvasedicionid eq '{id}'" for id in courses_id)

            # Realizamos la consulta OData
            query = f"/products?$filter=(statecode eq 0) and (({courses_id_str}))&$select=productid,bit_canvasedicionid"
            response = requests.get(config.resource_url + '/api/data/v9.0' + query, headers=headers_365)

            # Convertimos la respuesta en un DataFrame
            products_df = pd.DataFrame(response.json()['value'])
            
            # Creamos una lista con todos los productids
            productids = products_df['productid'].tolist()

            # Convertimos la lista de productids en una cadena para usar en la consulta OData
            productids_str = ' or '.join(f"_bit_curso_value eq '{id}'" for id in productids)

            # Realizamos la consulta OData
            query = f"/bit_cursodeedicions?$filter=(statecode eq 0) and (({productids_str}))&$select=_bit_curso_value&$expand=bit_Edicionprincipal($select=productnumber,productid,name)"
            response = requests.get(config.resource_url + '/api/data/v9.0' + query, headers=headers_365)

            # Convertimos la respuesta en un DataFrame
            editions_df = pd.DataFrame(response.json()['value'])

            # Expandimos la columna bit_Edicionprincipal
            expanded_editions_df = json_normalize(editions_df['bit_Edicionprincipal'])
            editions_df = pd.concat([editions_df, expanded_editions_df], axis=1)
            # Unimos los dataframes
            new_df = pd.merge(products_df, editions_df, left_on='productid', right_on='_bit_curso_value', how='left')
            print(new_df.columns)


            # Eliminamos los duplicados en bit_canvasedicionid manteniendo el que tiene menor cantidad de caracteres en bit_Edicionprincipal[name]
            new_df = new_df.loc[new_df.groupby('bit_canvasedicionid')['name'].idxmin()]

            # Renombramos las columnas
            new_df.rename(columns={
                'productid_x': 'TFM_productid',
                '_bit_curso_value': 'bit_curso',
                '_bit_curso_value@OData.Community.Display.V1.FormattedValue': 'bit_cursoname',
                'bit_canvasedicionid': 'bit_canvasedicionid',
                'productnumber': 'master_id',
                'productid_y': 'Edicionprincipal_productid',
                'name': 'master_concat'
            }, inplace=True)
            
            new_df.drop(columns = ["TFM_productid", "bit_curso", "bit_cursoname", "Edicionprincipal_productid"], inplace= True)
            return new_df


        def get_tutors_login_xlsx(all_tutors_df : pd.DataFrame):
            all_tutors_df = all_tutors_df.drop_duplicates(subset='id')
            # all_tutors_df.to_csv(f"{Directorio}/tutores_login.csv", index=False)
            # Creamos una copia de all_tutors_df con solo las columnas que necesitamos
            tutors_login_df = all_tutors_df[['id_calculado', 'sortable_name', 'sis_user_id']].copy()
            # Renombramos las columnas
            tutors_login_df.columns = ['dni_pasaporte', 'fullname', 'email']

            # Convertimos la columna 'fullname' para que la primera letra de cada palabra esté en mayúsculas y el resto en minúsculas
            tutors_login_df['fullname'] = tutors_login_df['fullname'].str.title()

            # Añadimos las columnas 'rol1' y 'rol2'
            tutors_login_df['rol1'] = 'Tutor'
            tutors_login_df['rol2'] = ''


            # Guardamos el DataFrame en un archivo Excel
            tutors_login_df.to_excel(f"{Resultados}/login_tfm.xlsx", index=False)
            tutors_login_df = tutors_login_df.astype(str)
            tutors_login_df.to_json(f"{Resultados}/login_tfm.txt", orient='records')
            
            print(f"Archivo listo para subir en dirección: {Resultados}/login_tfm.xlsx")
            pass
 
        
        def get_data_and_download_files(all_sections_df):
            """
            Fetches data from the API for each section in the provided DataFrame,
            filters and processes the data, and downloads the corresponding files.

            Args:
                all_sections_df (pandas.DataFrame): DataFrame containing section data.

            Returns:
                pandas.DataFrame: Processed DataFrame with downloaded files.

            Raises:
                None
            """
            headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
            unique_ids = all_sections_df['id'].drop_duplicates()
            all_data = []

            def fetch_data(section_id):
                url = f"{BASE_URL}/api/v1/sections/{section_id}/students/submissions?student_ids=all&include[]=assignment"
                response = requests.get(url, headers=headers)
                data = response.json()
                for item in data:
                    if 'assignment' in item and 'attachments' in item:
                        assignment_id = item['assignment_id'] if 'assignment_id' in item else None
                        assignment_name = item['assignment']['name'] if 'name' in item['assignment'] else None
                        attachment_display_name = item['attachments'][0]['display_name'] if item['attachments'] and 'display_name' in item['attachments'][0] else None
                        attachment_url = item['attachments'][0]['url'] if item['attachments'] and 'url' in item['attachments'][0] else None
                        all_data.append([section_id, assignment_id, assignment_name, attachment_display_name, attachment_url])

            with ThreadPoolExecutor() as executor:
                for _ in executor.map(fetch_data, unique_ids):
                    pass

            df = pd.DataFrame(all_data, columns=['section_id','assignment_id', 'assignment_name', 'attachment_display_name', 'attachment_url'])
            df.to_csv(f"{Directorio}/Docs/Docs1.csv", index=False, sep=';')
            # Drop duplicate assignment_id
            df = df.drop_duplicates(subset=['assignment_id', 'section_id'])
            df.to_csv(f"{Directorio}/Docs/Docs2.csv", index=False, sep=';')
            # Filter rows based on assignment_name
            df = df[df['assignment_name'].isin(["TFM", "Thesis", "Executive Summary", "Project", "Resumen ejecutivo con portada"])]

            # Create a new column _tipo_
            df['_tipo_'] = df['assignment_name'].map({"TFM": "T", "Thesis": "T", "Project": "T", "Executive Summary": "S", "Resumen ejecutivo con portada": "S"})
            df.to_csv(f"{Directorio}/Docs/Docs3.csv", index=False, sep=';')
            def download_file(row):
                section_folder = f"{Directorio}/Docs"
                os.makedirs(section_folder, exist_ok=True)
                url = row['attachment_url']
                response = requests.get(url, stream=True)
                # Change the file naming logic
                file_extension = row['attachment_display_name'].split('.')[-1]
                file_path = os.path.join(section_folder, f"{row['section_id']}_{row['_tipo_']}.{file_extension}")
                with open(file_path, 'wb') as out_file:
                    shutil.copyfileobj(response.raw, out_file)

            with ThreadPoolExecutor() as executor:
                for _ in executor.map(download_file, df.to_dict('records')):
                    pass

            print("Archivos descargados con éxito")
            return df

        
        def swap_order(name):
            # Comprueba si el valor es nulo
            if pd.isnull(name):
                return name
            # Divide el nombre por ', '
            elements = name.split(', ')
            # Invierte el orden de los elementos y únelos con ', '
            return ', '.join(elements[::-1])


        def get_Teams_TFM_xlsx(all_sections_df: pd.DataFrame, all_tutors_df: pd.DataFrame, parent_product_df: pd.DataFrame):
            Teams_TFM = all_sections_df[[
                                        'id','course_id','name','created_at','total_students', 'student_id_1','student_sortable_name_1',
                                        'student_id_2','student_sortable_name_2', 'student_id_3','student_sortable_name_3','student_id_4','student_sortable_name_4','course_name', 'course_code', 'tutor_id'
                                        ]].copy()
            Teams_TFM.rename(columns={'id': 'group_id', 'course_code' : 'edition'}, inplace=True)
            # Teams_TFM = Teams_TFM[Teams_TFM['tutor_id'].notna()]
            Teams_TFM['tutor_id'] = Teams_TFM['tutor_id'].astype(str)
            Teams_TFM = Teams_TFM.merge(all_tutors_df[['id', 'id_calculado', 'sortable_name', 'email']], left_on='tutor_id', right_on='id', how='left')
            Teams_TFM['created_at'] = pd.to_datetime(Teams_TFM['created_at']).dt.strftime('%Y/%m/%d %H:%M')
            Teams_TFM = Teams_TFM.merge(tfm_courses_df[['course_id', 'year']], on='course_id', how='left')
            Teams_TFM.insert(loc=Teams_TFM.columns.get_loc("course_name")+1, column='edition_name', value=Teams_TFM['course_name'])
            Teams_TFM.drop(columns=['id','tutor_id'], inplace=True)
            Teams_TFM.rename(columns={'id_calculado': 'tutor_id', 'sortable_name' : 'tutor_name', 'email' : 'tutor_email'}, inplace=True)
            Teams_TFM = Teams_TFM.drop_duplicates(subset=['tutor_id', 'group_id'])
            Teams_TFM['tutor_id'] = Teams_TFM['tutor_id'].replace(np.nan, "")
            parent_product_df['bit_canvasedicionid'] = parent_product_df['bit_canvasedicionid'].astype(str)
            Teams_TFM['course_id'] = Teams_TFM['course_id'].astype(str)
            Teams_TFM = pd.merge(Teams_TFM, parent_product_df, left_on='course_id', right_on='bit_canvasedicionid', how='left')
            cols = list(Teams_TFM.columns)

            # Eliminamos las columnas que se van a mover de la lista
            cols.remove('master_id')
            cols.remove('master_concat')

            # Insertamos las columnas en las posiciones deseadas
            cols.insert(14, 'master_id')
            cols.insert(16, 'master_concat')

            # Reindexamos el DataFrame
            Teams_TFM = Teams_TFM[cols]
            Teams_TFM.drop(columns=['bit_canvasedicionid'], inplace=True)
            for i in range(1, 5):
                Teams_TFM[f'student_sortable_name_{i}'] = Teams_TFM[f'student_sortable_name_{i}'].apply(swap_order)
            Teams_TFM.to_excel(f"{Resultados}/Teams_TFM.xlsx", index = False)
            Teams_TFM = Teams_TFM.astype(str)
            Teams_TFM.to_json(f"{Resultados}/Teams_TFM.txt", orient='records')
            print(f"Archivo listo para subir en dirección: {Resultados}/Teams_TFM.xlsx")
            



        #------------------------------------------MAIN--------------------------------------------------------------#
        loop = asyncio.get_event_loop()

        # DataFrames vacíos para almacenar todos los datos
        all_courses_df = pd.DataFrame()
        all_tutors_df = pd.DataFrame()
        all_students_df = pd.DataFrame()
        all_sections_df = pd.DataFrame()

        # Recuperamos los estudiantes, los tutores y las secciones para los cursos filtrados de tfm_courses_df
        tfm_courses_df = await get_tfm_courses(TFM_id)
        for course_id in tfm_courses_df['course_id']:
            students_df, tutors_df, sections_df = await asyncio.gather(
                get_students(course_id),
                get_tutors(course_id),
                get_course_sections(course_id, ['students', 'total_students','avatar_url'])
            )

            # Añadimos los nuevos datos a los DataFrames correspondientes
            all_students_df = pd.concat([all_students_df, students_df])
            all_tutors_df = pd.concat([all_tutors_df, tutors_df])
            all_sections_df = pd.concat([all_sections_df, sections_df])
            print(f"PROCESANDO CURSO CON ID {course_id} . . .")

        # Cerramos el bucle de eventos
        # loop.close()
        

        # Recuperamos Headers 365 para queries OData a Dynamics 365
        headers_365 = get_headers_365(config)

        # Filtramos secciones sin alumnos o con más de 4 y también filtramos aquellas que no contengan un digito en el nombre de la sección
        all_sections_df = all_sections_df[(all_sections_df['total_students'] > 0) & (all_sections_df['total_students'] < 5)]
        all_sections_df = all_sections_df[all_sections_df['name'].str.contains(r'\d', regex=True, na=False)]
        all_sections_df = expand_sections(all_sections_df)
        all_sections_df = get_course_code(all_sections_df, tfm_courses_df)
        parent_product_df = get_course_parent(all_sections_df, config, headers_365)

        # Recuperamos los ids de los tutores para cada sección
        print("Recuperando IDs calculados . . .")
        with ThreadPoolExecutor() as executor:
            all_sections_df['tutor_id'] = list(executor.map(get_section_tutor, all_sections_df['id']))

        all_tutors_df = get_tutors_id_365(all_tutors_df, config, headers_365)
        # Sacamos a CSV tablas principales
        all_courses_df.to_csv(f"{Directorio}/Cursos_TFM_ES24--GT24.csv", index=False, sep=';', encoding='utf-8')
        all_students_df.to_csv(f"{Directorio}/Estudiantes.csv", index=False, sep=';', encoding='utf-8')
        all_tutors_df.to_csv(f"{Directorio}/Tutores.csv", index=False, sep=';', encoding='utf-8')
        all_sections_df.to_csv(f"{Directorio}/Secciones.csv", index=False, sep=';', encoding='utf-8')
        parent_product_df.to_csv(f"{Directorio}/ParentProduct.csv", index=False, sep=';', encoding='utf-8')
        all_students_df.to_csv(f"{Directorio}/Students.csv", index=False, sep=',', encoding='utf-8')

        # Recuperamos Avatares
        download_avatars(all_students_df, f"{Resultados}/Avatares")
        print("!!!AVATARES RECUPERADOS!!!")

        get_data_and_download_files(all_sections_df)
        print("!!!PDFs RECUPERADOS!!!")

        
        
        get_tutors_login_xlsx(all_tutors_df)
        get_Teams_TFM_xlsx(all_sections_df, all_tutors_df, parent_product_df)

        print("SCRIPT FINALIZADO CON EXITO")
    pass
schedule.every().day.at("02:20:55").do(run_asyncio_job, job=job)

while True:
    schedule.run_pending()
    time.sleep(1)