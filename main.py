from docx2pdf import convert
from PIL import Image, ImageSequence
import pandas as pd
from sqlalchemy import create_engine
from dotenv import dotenv_values
import os
arr=[]
output_folder=r'c:\Users\AeroCool\Desktop\task\output'

def tiff_to_pdf(tiff_path: str, sink_folder:str) -> str: 
    pdf_path_end = os.path.basename(os.path.split(tiff_path)[0]+'_'+os.path.basename(tiff_path).split('.')[0]+'.pdf')
    pdf_path = os.path.join(sink_folder,pdf_path_end)
    if not os.path.exists(tiff_path): raise Exception(f'{tiff_path} does not find.')
    image = Image.open(tiff_path)

    images = []
    for i, page in enumerate(ImageSequence.Iterator(image)):
        page = page.convert("RGB")
        images.append(page)
    if len(images) == 1:
        images[0].save(pdf_path)
    else:
        images[0].save(pdf_path, save_all=True,append_images=images[1:])
    
    
    pat_id = os.path.basename(pdf_path).split('_')[0]
    return (pdf_path,pat_id)

def doc_to_pdf(doc_path:str,sink_folder:str):
    if not os.path.exists(doc_path):
        return
    sink_end = os.path.basename(os.path.split(doc_path)[0]+'_'+os.path.basename(doc_path).split('.')[0]+'.pdf')
    sink = os.path.join(sink_folder,sink_end)
    convert(doc_path,sink)
    pat_id = os.path.basename(sink).split('_')[0]

    return (sink,pat_id)

def _convert(path:str, sink:str):
    if path.endswith('.docx'):
        arr.append(doc_to_pdf(path,sink_folder=sink))
    elif path.endswith('.tiff'):
        arr.append(tiff_to_pdf(path,sink_folder=sink))



for dir,subdir,file in os.walk(r'c:\Users\AeroCool\Desktop\task\input'):
    if not subdir:
        for f in file:
            _convert(os.path.join(dir,f),output_folder)



df1 = pd.read_csv('demographics.csv')

df2 = pd.DataFrame(arr,columns=['path','patient_id'])

df1['patient_id'] = df1['patient_id'].astype(int)
df2['patient_id'] = df2['patient_id'].astype(int)

merged_df = pd.merge(df1, df2, on='patient_id', how='inner')

config = dotenv_values(".env")
server_name = config['server_name']
database_name = config['database_name']


engine = create_engine(f'mssql+pyodbc://{server_name}/{database_name}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')

# Save the merged DataFrame to SQL Server
merged_df.to_sql('merged_data', engine, index=False, if_exists='append')

# Close the database connection
engine.dispose()