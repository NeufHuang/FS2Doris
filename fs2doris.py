import argparse
import datetime
import logging
import os

from dotenv import load_dotenv
from pydoris.doris_client import *

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fe_host = os.getenv('FE_HOST')
fe_http_port = os.getenv('FE_HTTP_PORT')
fe_query_port = os.getenv('FE_QUERY_PORT')
username = os.getenv('DORIS_USERNAME')
passwd = os.getenv('DORIS_PASSWD')
db = os.getenv('DORIS_DB')

feishu_app_id = os.getenv('FEISHU_APP_ID')
feishu_app_secret = os.getenv('FEISHU_APP_SECRET')

doris_client = DorisClient(fe_host=fe_host,
                           fe_query_port=fe_query_port,
                           fe_http_port=fe_http_port,
                           username=username,
                           password=passwd,
                           db=db)


def get_arguments():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--url', type=str, help='Feishu doc url')
    parser.add_argument('--columns', type=str, help='Feishu columns range or name mapping')
    parser.add_argument('--rows', type=str, default=None, help='Feishu sheet rows range')
    parser.add_argument('--table_name', type=str, help='Doris table name')
    parser.add_argument('--table_module', type=str, default='Duplicate', help='Doris table module')
    parser.add_argument('--table_replace', type=str, default=None, help='Doris table repeat replacement')
    parser.add_argument('--table_bucket', type=str, default=None, help='Doris table bucket')
    parser.add_argument('--table_database', type=str, default='ODS', help='Doris databases')
    parser.add_argument('--batch_size', type=int, default=500, help='Batch rows')
    return parser.parse_args()


def write_to_doris(data_df: pd.DataFrame, table_name: str, table_model: str,
                   table_module_key=None,
                   distributed_hash_key=None,
                   buckets=None,
                   table_properties=None,
                   field_mapping: list[tuple] = None,
                   repeat_replacement: bool = None):
    replace_table = repeat_replacement
    if replace_table is None:
        replace_table = False
    elif replace_table:
        doris_client.execute(f"DROP TABLE  IF EXISTS {table_name}")

    doris_client.db_operator.create_table_from_df(replace_table, data_df, table_name, table_model,
                                                  table_module_key,
                                                  distributed_hash_key,
                                                  buckets,
                                                  table_properties,
                                                  field_mapping
                                                  )
    json_data = data_df.to_json(orient='records')

    return doris_client.write(table_name, json_data,
                              options=WriteOptions()
                              .set_json_format()
                              .set_option('strip_outer_array', 'true')
                              .set_option('fuzzy_parse', 'true')
                              )


def df_to_doris(df: pd.DataFrame, table_name, table_module=None, bucket=3, db_name=None, table_replace=None):
    field_maping = [
        ("record_id", "varchar(50)"),
        ("ETL_DATE", "datetime")
    ]
    # 去掉列名为空的,减少因用户编辑表结构导致的报错 --20251009
    df = df.loc[:, df.columns.notnull()]
    field_list = df.columns.get_level_values(0)
    logger.info('字段名: %s', field_list.tolist())

    for field in field_list:
        if field.endswith('_date'):
            date_str = (f'{field}', 'datetime')
            field_maping.append(date_str)
        if field.endswith('_str'):
            date_str = (f'{field}', 'string')
            field_maping.append(date_str)
        if field.endswith('_num'):
            date_str = (f'{field}', 'double')
            field_maping.append(date_str)

    if table_replace in ['true', 'True', '1']:
        replacement = True
        logger.info(f"{table_name} 表结构替换中...")
    else:
        replacement = False

    result = write_to_doris(df, f"{db_name}.{table_name}", table_module, ['record_id'],
                            distributed_hash_key=['record_id'],
                            buckets=bucket,
                            field_mapping=field_maping,
                            table_properties={"replication_allocation": "tag.location.default: 3"},
                            repeat_replacement=replacement)

    if result:
        logger.info(f"{db_name}.{table_name} 写入成功！")
    else:
        raise Exception(f"{db_name}.{table_name} 写入失败！")


def getFeishuToken():
    payload = {'app_id': feishu_app_id, 'app_secret': feishu_app_secret}
    response_post = requests.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal', payload)
    if response_post.status_code == 200:
        data_post = response_post.json()
        feishutoken = data_post.get('tenant_access_token')

        return {
            'Authorization': 'Bearer ' + feishutoken,
            'Content-Type': 'application/json; charset=utf-8'
        }
    else:
        raise Exception(f"FeiShuToken 请求失败！code: {response_post.status_code}")


# 多维表格  '流程', '单向关联', '双向关联'   等字段复杂类型无法实现, '直属上级', '工号'需要对应权限
def search_bitable(app_token, table_id, page_token, view_id=None, columns=None, automatic_fields=False, batch_size=500):
    page_size = min(batch_size, 500)
    url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search?page_size={page_size}&page_token={page_token}'

    headers = getFeishuToken()

    body = {
        "view_id": view_id,
        "field_names": columns,
        "automatic_fields": automatic_fields
    }
    body = json.dumps(body, ensure_ascii=False)

    try:
        response_get = requests.post(url, headers=headers, data=body)
        response_get.raise_for_status()
        bitable_data = response_get.json()
        if bitable_data.get('error') is not None:
            raise Exception(f"Error: {str(bitable_data['error'])}")
        #  logger.info(str(bitable_data)[:1000])

        return bitable_data
    except requests.RequestException as e:
        raise Exception(f"\n 请求失败: {e}")


def get_sheet_data(doc_id, sheet_id, rows, columns):
    url = f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{doc_id}/values_batch_get?ranges={sheet_id}!{columns},{sheet_id}!{rows}&valueRenderOption=ToString&dateTimeRenderOption=FormattedString'

    try:
        headers = getFeishuToken()
        response_get = requests.get(url, headers=headers)
        response_get.raise_for_status()
        json_data = response_get.json()

        # 检查数据是否成功获取
        if json_data:
            return json_data
        else:
            raise Exception("Missing expected keys in the JSON response.")

    except requests.RequestException as e:
        raise Exception(f"\n请求失败: {e}")


def parse_url_to_id(url: str):
    sheet_pattern1 = r'https://[^/]+\.feishu\.cn/sheets/([^?]+)\?sheet=([^\s&]+)'
    sheet_pattern2 = r'https://[^/]+\.feishu\.cn/sheets/([^?]+)'
    base_pattern = r'https://[^/]+\.feishu\.cn/base/([^?]+)\?table=([^\s&]+)&view=([^\s&]+)'

    match = re.match(sheet_pattern1, url)
    if match:
        table_id, sheet_id = match.groups()
        urlinfo = {'type': 'sheet', 'table_id': table_id, 'sheet_id': sheet_id}
        logger.info(f"匹配到普通表格: {urlinfo}")

        return urlinfo

    match = re.match(sheet_pattern2, url)
    if match:
        table_id = match.groups()[0]
        query_url = f'https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{table_id}/sheets/query'
        headers = getFeishuToken()
        try:
            response_get = requests.get(query_url, headers=headers)
            response_get.raise_for_status()
            sheet_data = response_get.json()
            sheet_id = sheet_data['data']['sheets'][0]['sheet_id']
        except requests.RequestException as e:
            raise Exception(f"\nGet SheetID failed: {e}")
        urlinfo = {'type': 'sheet_without_id', 'table_id': table_id, 'sheet_id': sheet_id}
        logger.info(f"匹配到普通表格，默认使用第一个sheet: {urlinfo}")

        return urlinfo

    match = re.match(base_pattern, url)
    if match:
        app_token, table_id, view_id = match.groups()
        urlinfo = {'type': 'bitable', 'app_token': app_token, 'table_id': table_id, 'view_id': view_id}
        logger.info(f"匹配到多维表格: {urlinfo}")

        return urlinfo

    raise ValueError("不支持的URL格式")


def parse_bitable_data(field):
    def clean_text(text):
        return str(text).replace(" ", "").replace('\n', ' ').strip()

    def deep_parse(obj):
        if not isinstance(obj, (dict, list)):
            return clean_text(obj) if obj else None

        if isinstance(obj, dict):
            if "text" in obj:
                return clean_text(obj["text"])

            for key in ["value", "full_address", "url", "name", "en_name"]:
                if key in obj:
                    parsed = deep_parse(obj[key])
                    if parsed:
                        return parsed if isinstance(parsed, str) else "|".join(parsed)

            for v in obj.values():
                result = deep_parse(v)
                if result:
                    return result
            return None

        if isinstance(obj, list):
            results = []
            for item in obj:
                parsed = deep_parse(item)
                if parsed:
                    if isinstance(parsed, list):
                        results.extend(parsed)
                    else:
                        results.append(parsed)
            return "|".join(results) if results else None

        return None

    result = deep_parse(field)

    if isinstance(result, list):
        return "|".join(map(clean_text, filter(None, result)))
    return clean_text(result) if result else None


def bitable_data_df(data, columns_map):
    parsed_data = []
    for item in data['data']['items']:
        parsed_item = {
            'record_id': item['record_id'],
            'ETL_DATE': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        for cn_name, en_name in columns_map.items():
            raw_value = item['fields'].get(cn_name)
            if raw_value is not None:
                if en_name.endswith('_date'):
                    parsed_item[en_name] = datetime.datetime.fromtimestamp(raw_value / 1000.0).isoformat()
                else:
                    parsed_value = parse_bitable_data(raw_value)
                    parsed_item[en_name] = parsed_value if parsed_value not in (None, '', []) else None
            else:
                parsed_item[en_name] = None
        parsed_data.append(parsed_item)

    df = pd.DataFrame(parsed_data, columns=['record_id', 'ETL_DATE'] + list(columns_map.values()))
    df = df.dropna(how='all', subset=columns_map.values())

    return df


def clean_cell(cell):
    if cell is None:
        return None
    if isinstance(cell, str):
        return cell.replace('\t', '').replace('\n', ' | ').strip()
    return cell


def parse_sheet_data(json_data, sheet_id, rows, columns, columns_map=None):
    data = json_data['data']
    column_names = []
    rows_data = []
    if 'valueRanges' in data and columns_map is None:
        value_ranges = data['valueRanges']
        for value_range in value_ranges:
            if value_range['range'] == f'{sheet_id}!{columns}':
                column_names = [[clean_cell(item).replace(' ', '_') if item is not None else item for item in sublist] for
                           sublist in value_range['values']]
                column_names = [item[0] if isinstance(item, tuple) else item for item in column_names[0]]
            elif value_range['range'] == f'{sheet_id}!{rows}':
                rows_data = [[clean_cell(item) for item in sublist] for sublist in value_range['values']]
        df = pd.DataFrame(rows_data, columns=column_names).dropna(how='all')
    elif 'valueRanges' in data and columns_map is not None:
        value_ranges = data['valueRanges']
        for value_range in value_ranges:
            if value_range['range'] == f'{sheet_id}!{columns}':
                column_names = [[clean_cell(item).replace(' ', '_') if item is not None else item for item in sublist]
                                for sublist in value_range['values']]
                column_names = [item[0] if isinstance(item, tuple) else item for item in column_names[0]]
                column_names = [columns_map.get(name, name) for name in column_names]
            elif value_range['range'] == f'{sheet_id}!{rows}':
                rows_data = [[clean_cell(item) for item in sublist] for sublist in value_range['values']]
        df = pd.DataFrame(rows_data, columns=column_names).dropna(how='all')
    else:
        raise ValueError("无效数据!")

    return df


def get_feishu_data(url: str, columns=None, rows=None, batch_size=500):
    url_info = parse_url_to_id(url)
    all_dfs = []

    # 处理多维表格
    if url_info['type'] == 'bitable':
        app_token = url_info['app_token']
        table_id = url_info['table_id']
        view_id = url_info['view_id']

        columns_kv = columns.replace('，', ',').replace('：', ':')
        pairs = columns_kv.split(',')
        columns_cn = [pair.split(":")[0].strip() for pair in pairs]
        columns_map = {pair.split(":")[0].strip(): pair.split(":")[1].strip() for pair in pairs}

        page_token = ''

        while True:
            bitable = search_bitable(app_token, table_id, page_token, view_id, columns_cn, batch_size=batch_size)

            # logger.info(bitable)

            if 'data' in bitable and 'items' in bitable['data']:
                df = bitable_data_df(bitable, columns_map)
                all_dfs.append(df)

            if 'data' in bitable and 'page_token' in bitable['data'] and bitable['data']['page_token']:
                page_token = bitable['data']['page_token']
            else:
                break

        final_df = pd.concat(all_dfs, ignore_index=True)
        logger.info(final_df)

        return final_df

    # 处理普通表格
    else:
        if not rows:
            raise ValueError("普通表格必须指定rows参数")

        match = re.match(r'([A-Z]+)(\d+):([A-Z]+)(\d+)', rows)
        if not match:
            raise ValueError(f"无效的行范围格式: {rows}")

        start_col, start_row, end_col, end_row = match.groups()
        start_row, end_row = int(start_row), int(end_row)
        total_rows = end_row - start_row + 1

        num_batches = (total_rows + batch_size - 1) // batch_size
        columns_map = {}

        columns = columns.replace('，', ',').replace('：', ':')
        is_excel_range = re.match(r'^[A-Z]+\d+:[A-Z]+\d+$', columns)
        is_custom_field = re.match(r'^(?:\w+:\w+(?:,\s*\w+:\w+)*)$', columns)

        if columns and is_excel_range:
            logger.info(f"取{columns}作为列名，无需映射")
        elif columns and is_custom_field:
            start_row = start_row + 1
            columns_map = {pair.split(":")[0].strip(): pair.split(":")[1].strip() for pair in columns.split(',')}
            columns = f"{start_col}1:{end_col}1"
            logger.info(f"取{columns}作为列名，映射列名{columns_map}")

        else:
            raise ValueError("无效的列格式")

        # 分批获取数据
        for i in range(num_batches):
            logger.info(f"正在获取第{i + 1}批数据...")
            batch_range = f"{start_col}{start_row + i * batch_size}:{end_col}{min(start_row + (i + 1) * batch_size - 1, end_row)}"
            data = get_sheet_data(url_info['table_id'], url_info['sheet_id'], batch_range, columns)
            df = parse_sheet_data(data, url_info['sheet_id'], batch_range, columns, columns_map)

            if df.empty or df.isna().all().all():
                logger.info(f"第{i + 1}批无有效数据，停止获取")
                break
            all_dfs.append(df)
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df['ETL_DATE'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final_df.insert(0, 'record_id', final_df.index)
        logger.info(final_df)

        return final_df


def main():
    args = get_arguments()

    df = get_feishu_data(args.url, args.columns, args.rows, batch_size=args.batch_size)

    df_to_doris(df, table_name=args.table_name, table_module=args.table_module, bucket=args.table_bucket,
                db_name=args.table_database, table_replace=args.table_replace)

def test():
    """
	测试
    """
    df = get_feishu_data("https://xxx.feishu.cn/base/xxxxx?table=xxxxx",
                         "序号:serialNumber,资产编号:assetNumber,设备型号:assetModel,产品型号:productModel,UPH:uph",
                         "A1:E5000",
                         batch_size=200)
    # df = get_feishu_data("https://xxx.feishu.cn/base/xxxxx?table=xxxxx",
    #                      "A2:EK2",
    #                      "A7:EK10000",
    #                      batch_size=200)
    df_to_doris(df, table_name="FEISHU_PCBA_BIG_PLAN",table_module="Duplicate",table_replace='1',
               db_name="test_db")

if __name__ == '__main__':
    main()