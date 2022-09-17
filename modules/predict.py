import pandas as pd
import dill
import os
from datetime import datetime
import json


def find_last_file(dir):
    f_list = os.listdir(dir)
    f_l_dates = [datetime.strptime(f[10:14]+'-'+f[14:16]+'-'+f[16:18]+'-'+f[18:20]+'-'+f[20:22], "%Y-%m-%d-%H-%M") for f in f_list]
    y = f_l_dates.index(max(f_l_dates))
    file_name = f_list[y]
    return dir+'/'+file_name


#path = os.path.abspath(os.path.join('__file__','../..'))
#path = '/opt/airflow'
path = os.environ.get('PROJECT_PATH', '/opt/airflow')

def predict():
    with open(find_last_file(path + '/dags/data/models/'), 'rb') as file:
        model = dill.load(file)
    df_final = pd.DataFrame(columns = ['id', 'price', 'price_category'])
    data = os.listdir(path=path+'/dags/data/test/')
    for i in range(len(data)):
        with open(path+'/dags/data/test/'+data[i], 'rb') as file:
            form = json.load(file)
            df = pd.DataFrame(form, index=[""])
            xy = model.predict(df)
            df_mod = df[['id', 'price']]
            df_final = df_final.append(df_mod)
            df_final.loc[(df_final.id == df.id[0]), ['price_category']] = xy[0]

        pred_filename = f'{path}/dags/data/predictions/prediction_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
        df_final.to_csv(pred_filename)


if __name__ == '__main__':
    predict()
