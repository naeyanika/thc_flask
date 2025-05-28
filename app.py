import os
import io
import pandas as pd
from flask import Flask, render_template, request
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_ANON_KEY"]
SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
bucket = 'thc'

app = Flask(__name__)

# Helper untuk download file dari bucket Supabase
def download_file_from_bucket(filename):
    response = supabase.storage.from_(bucket).download(filename)
    if response.status_code == 200:
        return io.BytesIO(response.content)
    else:
        raise Exception(f"Failed to download {filename}: {response.text}")

# Helper untuk upload file ke bucket Supabase (overwrite)
def upload_file_to_bucket(filename, fileobj, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
    # Supabase-Py expects bytes, not a file-like object
    fileobj.seek(0)
    file_bytes = fileobj.read()
    # Use service role for overwrite (optional, but anon usually can also overwrite if public)
    supabase_service = create_client(SUPABASE_URL, SERVICE_ROLE_KEY)
    resp = supabase_service.storage.from_(bucket).upload(filename, file_bytes, content_type=content_type, upsert=True)
    return resp

# Add these helper functions at the top of the file
def format_no(no):
    try:
        if pd.notna(no):
            return f'{int(no):02d}.'
        else:
            return ''
    except (ValueError, TypeError):
        return str(no)

def format_center(center):
    try:
        if pd.notna(center):
            return f'{int(center):03d}'
        else:
            return ''
    except (ValueError, TypeError):
        return str(center)

def format_kelompok(kelompok):
    try:
        if pd.notna(kelompok):
            return f'{int(kelompok):02d}'
        else:
            return ''
    except (ValueError, TypeError):
        return str(kelompok)

# Modify your process() function:
@app.route("/", methods=["GET", "POST"])
def process():
    result_url = ""
    error = ""
    if request.method == "POST":
        try:
            # Download files dari bucket
            files_needed = ['THC.csv', 'DbSimpanan.csv', 'DbPinjaman.csv']
            dfs = {}
            for fname in files_needed:
                csv_bytes = download_file_from_bucket(fname)
                # Cek delimiter ';' atau ','
                try:
                    df = pd.read_csv(csv_bytes, delimiter=';', low_memory=False)
                except:
                    csv_bytes.seek(0)
                    df = pd.read_csv(csv_bytes, delimiter=',', low_memory=False)
                dfs[fname] = df

            # Process DbSimpanan
            df1 = dfs['DbSimpanan.csv']
            df1.columns = df1.columns.str.strip()
            
            temp_client_id = df1['Client ID'].copy()
            df1['Client ID'] = df1['Account No']
            df1['Account No'] = temp_client_id
            
            df1.columns = ['NO.', 'DOCUMENT NO.', 'ID ANGGOTA', 'NAMA', 'CENTER', 'KELOMPOK', 'HARI', 'JAM', 'SL', 'JENIS SIMPANAN'] + list(df1.columns[10:])
            
            df1['NO.'] = df1['NO.'].apply(format_no)
            df1['CENTER'] = df1['CENTER'].apply(format_center)
            df1['KELOMPOK'] = df1['KELOMPOK'].apply(format_kelompok)

            # Process DbPinjaman
            df2 = dfs['DbPinjaman.csv']
            df2.columns = df2.columns.str.strip()
            
            temp_client_id = df2['Client ID'].copy()
            df2['Client ID'] = df2['Loan No.']
            df2['Loan No.'] = temp_client_id
            
            df2.columns = ['NO.', 'DOCUMENT NO.', 'ID ANGGOTA', 'DISBURSE', 'NAMA', 'CENTER', 'KELOMPOK', 'HARI', 'JAM', 'SL', 'JENIS PINJAMAN'] + list(df2.columns[11:])
            
            df2['NO.'] = df2['NO.'].apply(format_no)
            df2['CENTER'] = df2['CENTER'].apply(format_center)
            df2['KELOMPOK'] = df2['KELOMPOK'].apply(format_kelompok)

            # Process THC
            df3 = dfs['THC.csv']
            df3.columns = df3.columns.str.strip()
            
            df3['DOCUMENT NO.'] = df3['DOCUMENT NO.'].fillna('N/A')
            df3['TRANS. DATE'] = pd.to_datetime(df3['TRANS. DATE'], format='%d/%m/%Y', errors='coerce')
            df3['ENTRY DATE'] = pd.to_datetime(df3['ENTRY DATE'], format='%d/%m/%Y', errors='coerce')

            # Continue processing as in your Streamlit code...
            # (Add the rest of the processing logic here)

            # Save results to Excel in memory
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                pivot_table4.to_excel(writer, sheet_name='Pinjaman', index=False)
                pivot_table5.to_excel(writer, sheet_name='Simpanan', index=False)
                df_pinjaman_na.to_excel(writer, sheet_name='Pinjaman NA', index=False)
                df_simpanan_na.to_excel(writer, sheet_name='Simpanan NA', index=False)
                df3_blank.to_excel(writer, sheet_name='THC Blank', index=False)
            
            output.seek(0)

            # Upload to bucket
            output_filename = "hasil_pivot.xlsx"
            upload_file_to_bucket(output_filename, output)
            result_url = f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{output_filename}"

            return render_template("done.html", result_url=result_url)
        except Exception as e:
            error = str(e)
    return render_template("index.html", error=error)

if __name__ == "__main__":
    app.run(debug=True)