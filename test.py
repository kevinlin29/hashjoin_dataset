import clickhouse_connect
import time
import pandas as pd

def benchmark(hash, probe, lim1, lim2, runs, client, test_case):
    runtime = 0
 
    st_process = time.time()
    for i in range(runs):
        print(i)
        #time.sleep(10)
        st_process = time.time()
        result = client.query('SELECT COUNT (*) as RES FROM {} INNER JOIN {} ON {}.{} = {}.{} and {}.id < {} and {}.id < {}'.format(probe, hash, probe, test_case, hash, test_case, probe, lim1,hash, lim2))
        et_process = time.time()
        res_process = et_process - st_process
        if(i >= 1):
            runtime += res_process
        #if i > 1:
        print(result.result_rows)
        rows = int(result.summary["read_bytes"]) / 1000000
        print("mb/s per second", rows/res_process)
        
    runtime /= (runs-1)
    print("Run {} joining {} : {} run average: {}".format(lim1, lim2, runs-1,runtime))
    return runtime

if __name__ == "__main__":
    client = clickhouse_connect.get_client(host='localhost', username='default', password='')
    probe_size = [0, 1000000, 5000000, 10000000, 50000000]
    #probe_size = [10000000]
    hash_size = [1000000, 5000000, 10000000, 50000000]
    #hash_size = [10000000]
    hash_table = "test_v4x1"
    probe_table = "test_v4"
    data = []
    for j in hash_size:
        subdata = []
        for i in probe_size:
            subdata.append(benchmark(hash_table, probe_table, i, j, 5, client, "short_string"))
            print(subdata)
        data.append(subdata)
    df = pd.DataFrame(data).T
    
    with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
        print(df)
        
    # rows are the different probe sizes
    # columns are the different hash sizes
    # slight variation run to run
    
    
    
        
    
