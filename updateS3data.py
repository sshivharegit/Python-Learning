import subprocess
import os
import json
import datetime
from datetime import datetime, timedelta
import time
import country_converter as coco

bucket_list = [ "s3://lo-werphat--enrichment/data/json/year=2022/month=05/day=07/hour=01/na_enriched.json"]

for i in bucket_list:
    bucketPath = i
    upload_path = i[:-16]
    localPath = '.'
    subprocess.check_output(['aws', 's3', 'cp', bucketPath, localPath])

    try:
        os.rename(i[-16:], 'na_enriched_temp.json')

        with open("na_enriched_temp.json", "r") as f:
            lines = f.readlines()

            for line in lines:
                file_dict = json.loads(line)
                file_dict.items()
                date_key = file_dict['datasets'][0]['date']
                temp_var = date_key.replace(".", ":")
                final_out = temp_var[:16] + temp_var[19:]
                file_dict['datasets'][0]['date'] = final_out


                with open('na_enriched_hold.json', 'a+') as f:
                    json.dump(file_dict, f)
                    f.write('\n')
            
            os.remove('na_enriched_temp.json')
            
            with open('na_enriched_hold.json', "r") as file:
                raw_data = {}
                lines = file.readlines()
                ab = len(lines)
                for i in range(ab):
                    first_raw = json.loads(lines[i])
                    data = first_raw['properties']
                    for item in data:
                        if item['key'] == 'country_code':
                            index_number = data.index(item)
                            aaa = item['value']
                            iso2_codes = coco.convert(names=aaa, to='ISO2')
                            print(aaa, iso2_codes)

                            first_raw['properties'][index_number]['value'] = iso2_codes

                            with open('na_enriched.json', 'a+') as f:
                                json.dump(first_raw, f)
                                f.write('\n')
                        else:
                            pass
            
        os.remove('na_enriched_hold.json')
        subprocess.check_output(['aws', 's3', 'cp', 'na_enriched.json', upload_path])
        os.remove('na_enriched.json')
    except:
        print(i)
        pass
