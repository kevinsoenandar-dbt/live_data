copy into {database}.{schema}.{table} from @~/sao/ files=('{file_name}') 
file_format = (type = 'csv' field_delimiter = ',' skip_header = 1);