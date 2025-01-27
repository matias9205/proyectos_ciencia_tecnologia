import pandas as pd
import os

# BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\ETL_proyectos_ciencia_tecnologia\\sample_files"
BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\ETL_proyectos_ciencia_tecnologia"
sample_files_list = os.listdir(BASE_DIR+"\\sample_files")

xlsx_dir = "XLSX"
xlsx_dir_url = BASE_DIR+"\\XLSX"

folder_path = os.path.join(BASE_DIR, xlsx_dir)
if not os.path.exists(folder_path):
    os.makedirs(folder_path)  # Crear la carpeta
    print(f"Carpeta '{folder_path}' creada en {folder_path}")
else:
    print(f"Carpeta '{folder_path}' ya existe en {BASE_DIR}")

# def convert_csv_to_xlsx(df_csv):
#     # for file_ in dir_:
#     #     print("-------------------------------------------------------------------------")
#     #     data = pd.read_csv(os.path.join(csvs_dir_url, file_))
#     #     xlsx_file = os.path.join(xlsx_dir_url, file_.replace(".csv", ".xlsx"))
#     #     writer = pd.ExcelWriter(file, engine='xlsxwriter')
#     #     data.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)
#     #     workbook = writer.book
#     #     worksheet = writer.sheets['Sheet1']
#     #     (max_row, max_col) = data.shape
#     #     column_settings = []
#     #     for header in data.columns:
#     #         column_settings.append({'header': header})
#     #     worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
#     #     worksheet.set_column(0, max_col - 1, 12)
#     #     writer.close()
#     #     print(f"{xlsx_file} was created successfully")

#     writer = pd.ExcelWriter(df_csv, engine='xlsxwriter')
#     df_csv.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)
#     workbook = writer.book
#     worksheet = writer.sheets['Sheet1']
#     (max_row, max_col) = df_csv.shape
#     column_settings = []
#     for header in df_csv.columns:
#         column_settings.append({'header': header})
#     worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
#     worksheet.set_column(0, max_col - 1, 12)
#     writer.close()
#     print(f"{xlsx_file} was created successfully")

def read_files(dir_):
    for file in dir_:
        if 'proyectos_anios' in file:
            proyectos_anios_dir = BASE_DIR+"\\sample_files\\"+file
            proyectos_anios_list = os.listdir(proyectos_anios_dir)
            first_file = proyectos_anios_dir+"\\"+proyectos_anios_list[0]
            print(f"------------------first file: {first_file}------------------------------")
            df = pd.read_csv(first_file, sep=';')
            print("------------------df.columns------------------------------")
            print(df.columns)
            for file_ in proyectos_anios_list:
                xlsx_file = os.path.join(xlsx_dir_url+"\\proyectos_anios", file_.replace(".csv", ".xlsx"))
                writer = pd.ExcelWriter(xlsx_file, engine='xlsxwriter')
                df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)
                workbook = writer.book
                worksheet = writer.sheets['Sheet1']
                (max_row, max_col) = df.shape
                column_settings = []
                for header in df.columns:
                    column_settings.append({'header': header})
                worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
                worksheet.set_column(0, max_col - 1, 12)
                writer.close()
                print(f"{xlsx_file} was created successfully")
        else:
            print(f"------------------------------SAMPLE CSV FILE: {file}------------------------------------")
            print(BASE_DIR+"\\sample_files\\"+file)
            df = pd.read_csv(BASE_DIR+"\\sample_files\\"+file, sep=';')
            print("------------------df.columns------------------------------")
            print(df.columns)
            xlsx_file = os.path.join(xlsx_dir_url, file.replace(".csv", ".xlsx"))
            writer = pd.ExcelWriter(xlsx_file, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            (max_row, max_col) = df.shape
            column_settings = []
            for header in df.columns:
                column_settings.append({'header': header})
            worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
            worksheet.set_column(0, max_col - 1, 12)
            writer.close()
            print(f"{xlsx_file} was created successfully")

if __name__ == "__main__":
    read_files(sample_files_list)