import pandas as pd
import os


def check_folder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


# Get data from file and return DataFrame
def extract(source_folder_path, names, filename,skip_rows):
    file = open(source_folder_path + '/' + filename, "r+")
    contents = file.read()
    # Change content of the file for easy df creation
    replaced_contents = contents.replace('{', '"').replace('}', '"')
    file = open(source_folder_path + filename, "w")
    file.write(replaced_contents)
    df = pd.read_csv(source_folder_path + filename,
                     names=names,
                     quotechar='"',
                     sep='\t',
                     skiprows=skip_rows)
    # Keep source file unchanged
    file.write(contents)
    file.close()

    return df


# Modify DataFrame and create one row for each order item
def transform(df, to_be_parsed, to_be_dropped):
    df = (df.set_index(df.columns.drop(to_be_parsed, 1).tolist())
              .ix[:, 0].str.split(',', expand=True) # ask Mr. Coder and replace by to_be_parsed
              .stack()
              .reset_index()
              .rename(columns={0: to_be_parsed})
              .loc[:, df.columns]
          )
    # Modify DataFrame and split column to get Database ready values for easy manipulation
    parsed = to_be_parsed.split("_")
    df[[parsed[0], parsed[1]]] = df[to_be_parsed].str.split(n=1, expand=True)
    df = df.drop(columns=[to_be_parsed])
    df[parsed[0]] = df[parsed[0]].str.replace("'", '').str.replace(":", '')
    if to_be_dropped != '':
        df = df.drop(columns=[to_be_dropped])
    return df


# Write Processed File
def load(df, dest_folder_path, col_name, filename):
    col_name = col_name.replace("Item_Quantity", "")+'_'
    print('---')
    df.to_csv(dest_folder_path + '/' + col_name + filename, sep='\t')


def main() -> object:
    source_folder_path = 'SourceFiles'
    dest_folder_path = 'ProcessedFiles'
    filename_c = 'Complaints.tsv'
    names_c = ['InvoiceId', 'Item']
    names_s = ['InvoiceId', 'CustomerId', 'Item_QuantitySold', 'BatchId']
    to_be_parsed_s = 'Item_QuantitySold'
    filename_s = 'Sales.tsv'
    names_pl = ['ProductionUnitId', 'BatchId', 'Item_QuantityProduced', 'Item_QuantityDiscarded']
    to_be_parsed_pl = 'Item_QuantityProduced'
    to_be_dropped_pl = 'Item_QuantityDiscarded'
    filename_pl = 'Production_logs.tsv'
    names_dpl = ['ProductionUnitId', 'BatchId', 'Item', 'QuantityDiscarded']
    names_ppl = ['ProductionUnitId', 'BatchId', 'Item', 'QuantityProduced']
    filename_dpl = 'Discarded_Production_logs.tsv'
    filename_ppl = 'Produced_Production_logs.tsv'

    check_folder(dest_folder_path)

    # Getting complaints data
    df_c = extract(source_folder_path, names_c, filename_c, 0)

    # Getting sales data
    df = extract(source_folder_path, names_s, filename_s, 0)
    df_s = transform(df, to_be_parsed_s, '')

    # Merge sales and complaint data
    df_sc = pd.merge(df_c, df_s, on=['InvoiceId', 'Item'], how='inner').drop_duplicates()
    df_sc = df_sc.rename(columns={"QuantitySold": "DefectiveQuantitySold"})
    df_sc = df_sc.drop(columns=['InvoiceId', 'CustomerId'])
    df_sc = df_sc.groupby(['BatchId', 'Item']).agg(lambda x: pd.to_numeric(x, errors='coerce').sum()).reset_index()

    # Getting production logs
    df = extract(source_folder_path, names_pl, filename_pl, 0)
    df_pl = transform(df, to_be_parsed_pl, to_be_dropped_pl)
    load(df_pl, dest_folder_path, to_be_parsed_pl, filename_pl)
    df_pl = transform(df, to_be_dropped_pl, to_be_parsed_pl)
    load(df_pl, dest_folder_path, to_be_dropped_pl, filename_pl)

    # Transforming production logs
    df_dpl = extract(dest_folder_path, names_dpl, filename_dpl, 1)
    df_ppl = extract(dest_folder_path, names_ppl, filename_ppl, 1)
    df_pl = pd.merge(df_ppl, df_dpl, on=['ProductionUnitId', 'BatchId', 'Item'], how='left')
    df_final = pd.merge(df_pl, df_sc, on=['BatchId', 'Item'], how='left').fillna(0)
    load(df_final, dest_folder_path, 'processed', 'final.tsv')
    return 1


main()
