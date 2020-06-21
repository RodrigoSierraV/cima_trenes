import csv
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt


def process_excel(file):
    print("Reading File", datetime.now())
    file_xls = pd.ExcelFile(file)
    sheets = file_xls.sheet_names
    print("FILE READ", datetime.now())
    events = sheets[0]
    train = sheets[1]

    pd.options.mode.chained_assignment = None
    res_ini = dt.timedelta(0,10)
    res_fin = dt.timedelta(0,250)

    values = [0,0,0,0,0,0,0,0,0,0,0,0,50.5,44.7,39.4,34.6,30.2,26.2,22.5,19.1,16.1,13.4,
              10.9,8.6,6.6,4.8,3.2,1.9,0.8,0,-0.6,-1,-1.2,-1.3,-1.2,-1,-0.5,0.1,1.1,2.5,4.3,6.3,9.3]
    header_op_res = ["LApeak (TH) [dB]","LASmax (TH) [dB]","LASmin (TH) [dB]","LASeq (TH) [dB]","LApeak (TH) [dB]","LAImax (TH) [dB]",
                     "LAImin (TH) [dB]","LAIeq (TH) [dB]","LCpeak (TH) [dB]","LCSmax (TH) [dB]","LCSmin (TH) [dB]","LCSeq (TH) [dB]",
                     "20 Hz","25 Hz","31.5 Hz","40 Hz","50 Hz","63 Hz","80 Hz","100 Hz","125 Hz","160 Hz","200 Hz","250 Hz","315 Hz",
                     "400 Hz","500 Hz","630 Hz","800 Hz","1000 Hz","1250 Hz","1600 Hz","2000 Hz","2500 Hz","3150 Hz","4000 Hz",
                     "5000 Hz","6300 Hz","8000 Hz","10000 Hz","12500 Hz","16000 Hz","20000 Hz"]

    def extrange(num_list):
        return np.around(10*np.log10(np.average(list(map(lambda x: 10**(x/10), num_list)))), 2)

    op_list = [np.max, np.max, np.min, extrange, np.max, np.max, np.min, extrange, np.max, np.max, np.min, extrange] + [extrange for i in range(len(values)-12)]

    df_events = file_xls.parse(sheet_name=events, usecols="B,C,H,L", parse_dates=True)

    df_train = file_xls.parse(sheet_name=train, header=[2,3,4], parse_dates=True)

    df_train.iloc[:,1] = pd.to_datetime(df_train.iloc[:,1])
    df_event = df_train[(df_train.iloc[:,1] >= df_events.iloc[0,1]) & (df_train.iloc[:,1] <= df_events.iloc[0,2])]
    df_event.insert(0, 'operator', df_events.iloc[0,0])
    df_event.insert(0, 'station', events[:-5])
    df_event.insert(0, 'event', 1)

    op_results = []
    for j in range(len(op_list)):
        if j <= 11:
            op_results.append(op_list[j](df_event.iloc[:,5+j]))
        else:
            op_results.append(op_list[j](df_event.iloc[:,38+j]))
    op_results_final = [np.around(op_results[i]+values[i],2) for i in range(len(values))]
    df_res_op = pd.DataFrame(op_results_final).T
    df_res_op.columns = header_op_res
    df_res_op.insert(0, 'Date & Time', df_event.iloc[0,4])
    df_res_op.insert(0, 'operator', df_event.iloc[0,2])
    df_res_op.insert(0, 'station', events[:-5])
    df_res_op.insert(0, 'event', 1)

    for i in range(1, len(df_events['Tren'])):
        df_event_i = df_train[(df_train.iloc[:,1] >= df_events.iloc[i,1]) & (df_train.iloc[:,1] <= df_events.iloc[i,2])]
        df_event_i.insert(0, 'operator', df_events.iloc[i,0])
        df_event_i.insert(0, 'station', events[:-5])
        df_event_i.insert(0, 'event', i + 1)
        df_event = pd.concat([df_event, df_event_i], axis=0)
        op_results_i = []
        for j in range(len(op_list)):
            if j <= 11:
                op_results_i.append(op_list[j](df_event_i.iloc[:,5+j]))
            else:
                op_results_i.append(op_list[j](df_event_i.iloc[:,38+j]))
        op_results_final_i = [np.around(op_results_i[z]+values[z],2) for z in range(len(values))]
        df_res_op_i = pd.DataFrame(op_results_final_i).T
        df_res_op_i.columns = header_op_res
        df_res_op_i.insert(0, 'Date & Time', df_event_i.iloc[0,4])
        df_res_op_i.insert(0, 'operator', df_event_i.iloc[0,2])
        df_res_op_i.insert(0, 'station', events[:-5])
        df_res_op_i.insert(0, 'event', i + 1)
        df_res_op = pd.concat([df_res_op, df_res_op_i], axis=0)
    df_event.set_index('event', inplace=True)
    df_res_op.set_index('event', inplace=True)

    if isinstance(df_events.iloc[0,3], float):
        df_residual = df_train[(df_train.iloc[:,1] >= df_events.iloc[0,2] + res_ini) & (df_train.iloc[:,1] <= df_events.iloc[0,2] + res_fin)]
    else:
        df_residual = df_train[(df_train.iloc[:,1] <= df_events.iloc[0,1] - res_ini) & (df_train.iloc[:,1] >= df_events.iloc[0,1] - res_fin)]
    df_residual.insert(0, 'operator', df_events.iloc[0,0])
    df_residual.insert(0, 'station', events[:-5])
    df_residual.insert(0, 'event', 1)

    op_results_residual = []
    for j in range(len(op_list)):
        if j <= 11:
            op_results_residual.append(op_list[j](df_residual.iloc[:,5+j]))
        else:
            op_results_residual.append(op_list[j](df_residual.iloc[:,38+j]))
    op_results_residual_final = [np.around(op_results_residual[i]+values[i],2) for i in range(len(values))]
    df_res_op_residual = pd.DataFrame(op_results_residual_final).T
    df_res_op_residual.columns = header_op_res
    df_res_op_residual.insert(0, 'Date & Time', df_residual.iloc[0,4])
    df_res_op_residual.insert(0, 'operator', df_residual.iloc[0,2])
    df_res_op_residual.insert(0, 'station', events[:-5])
    df_res_op_residual.insert(0, 'event', 1)

    for i in range(1, len(df_events['Tren'])):
        if isinstance(df_events.iloc[i,3], float):
            df_res_i = df_train[(df_train.iloc[:,1] >= df_events.iloc[i,2] + res_ini) & (df_train.iloc[:,1] <= df_events.iloc[i,2] + res_fin)]
        else:
            df_res_i = df_train[(df_train.iloc[:,1] <= df_events.iloc[i,1] - res_ini) & (df_train.iloc[:,1] >= df_events.iloc[i,1] - res_fin)]
        df_res_i.insert(0, 'operator', df_events.iloc[i,0])
        df_res_i.insert(0, 'station', events[:-5])
        df_res_i.insert(0, 'event', i + 1)
        df_residual = pd.concat([df_residual, df_res_i], axis=0)
        op_results_residual_i = []
        for j in range(len(op_list)):
            if j <= 11:
                op_results_residual_i.append(op_list[j](df_res_i.iloc[:,5+j]))
            else:
                op_results_residual_i.append(op_list[j](df_res_i.iloc[:,38+j]))
        op_results_residual_final_i = [np.around(op_results_residual_i[z]+values[z],2) for z in range(len(values))]
        df_res_op_residual_i = pd.DataFrame(op_results_residual_final_i).T
        df_res_op_residual_i.columns = header_op_res
        df_res_op_residual_i.insert(0, 'Date & Time', df_res_i.iloc[0,4])
        df_res_op_residual_i.insert(0, 'operator', df_res_i.iloc[0,2])
        df_res_op_residual_i.insert(0, 'station', events[:-5])
        df_res_op_residual_i.insert(0, 'event', i + 1)
        df_res_op_residual = pd.concat([df_res_op_residual, df_res_op_residual_i], axis=0)
    df_residual.set_index('event', inplace=True)
    df_res_op_residual.set_index('event', inplace=True)
    print("WRITING FILE...", datetime.now())
    with pd.ExcelWriter(file, engine="openpyxl", mode='a') as writer:
        df_event.to_excel(writer, sheet_name='Eventos de Tren', float_format="%.2f")
        df_res_op.to_excel(writer, sheet_name='Calculos Eventos', float_format="%.2f")
        df_residual.to_excel(writer, sheet_name='Residual de Tren', float_format="%.2f")
        df_res_op_residual.to_excel(writer, sheet_name='Calculos Residual', float_format="%.2f")
    print("Process Excel Done", datetime.now())
    return file
