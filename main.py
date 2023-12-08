from docx2pdf import convert
from PIL import Image, ImageSequence
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import dotenv_values
from fpdf import FPDF
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

def txt2pdf(source:str, sink:str):
    pdf = FPDF()   
  
    pdf.add_page()
    
    pdf.set_font("Arial", size = 15)
    
    # open the text file in read mode

    f = open(source, "r")
    
    # insert the texts in pdf
    for x in f:
        pdf.cell(200, 10, txt = x, ln = 1, align = 'C')
    
    # save the pdf with name .pdf
    sink_end = os.path.basename(os.path.split(source)[0]+'_'+os.path.basename(source).split('.')[0]+'.pdf')
    sink = os.path.join(sink,sink_end)
    
    pdf.output(sink)

    pat_id = os.path.basename(sink).split('_')[0]
    return (sink,pat_id)

def _convert(path:str, sink:str):
    if path.endswith('.docx'):
        arr.append(doc_to_pdf(path,sink_folder=sink))
    elif path.endswith('.tiff'):
        arr.append(tiff_to_pdf(path,sink_folder=sink))
    elif path.endswith('.txt'):
        arr.append(txt2pdf(path,sink))
    


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

# Get sql table to compare with new data frame
table_name = "merged_data"
query = text(f"SELECT * FROM {table_name}")

with engine.connect() as connection:
    result = connection.execute(query)
    rows = result.fetchall()

# Load sql table into a Pandas DataFrame
column_names = result.keys()
df_tbl = pd.DataFrame(rows, columns=column_names)

# Extract data which is not in db yet
new_rows = pd.merge(merged_df,df_tbl,how='left',on='path')
res_df = merged_df[merged_df['path'].isin(new_rows[new_rows['row_id'].isna()]['path'])]

# Load to sql new data
res_df.to_sql('merged_data',con=engine, index=False ,if_exists='append')  

# Close the database connection
engine.dispose()