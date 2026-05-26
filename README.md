
# 一、说明

多年前古法编程写的，用于dolphinscheduler定时同步飞书表格到Doris，实现多用户协作填写表格同步到数仓，代码有些地方可能不太合理，需要的自行优化。
1. 功能说明

   1. 支持自动建表
   2. 支持表替换(删除表后重建)
   3. 可选表模型,  自动指定key列为record\_id
   4. 默认增加ETL\_DATE字段
   5. 支持通过url识别表格类型
   6. 优化表格自定义列名映射方式,  兼容旧版本二维表格列名
2. 使用说明

   1. 使用前提,  安装所需依赖,  在脚本同目录下新建`.env`文件,  填写相关的参数

      ```yaml
      # .env config

      # feishu config
      FEISHU_APP_ID=
      FEISHU_APP_SECRET=

      # doris config
      FE_HOST=doris
      FE_HTTP_PORT=8030
      FE_QUERY_PORT=9030
      DORIS_USERNAME=doris
      DORIS_PASSWD=doris
      DORIS_DB=doris
      ```
   2. url : 必填项,  文档的链接,  二维表格或多维表格,  需注意:  二维表格链接中没有sheet时默认取第一个sheet,  多维表格连接必须有view
   3. table\_name : 必填项,  Doris表名, 自动以该名字建表
   4. columns :  必填项,  格式为"飞书列名1:数仓列名1,  飞书列名2:数仓列名2,  ......" 作为飞书列名和Doris的列名进行映射,  通过这个参数匹配云文档中列名和数仓列名

      - 特别注意:

        - 云文档中涉及的列名,  必须与该参数保持一致,  否则获取不到数据
        - 二维表格使用列名映射时,  默认匹配表格rows参数中的第一行, 如rows为"A1:G100"时,  默认取"A1:G1"作为表的列名进行映射
        - 旧版列名方案兼容:   二维表格可用"A2:G2"这种形式指定表中预留的字段名,  指定云文档中的行作为字段名,  当云文档中字段名为全英文时可用此方式,  无需做表名映射
        - 自动建表字段类型均为String,  日期类型需要把数仓字段名设为以'\_date'结尾,  建表时会将该字段设为**datetime**类型,  如未按此规则默认字符串类型,  多维表格的日期为时间戳
          目前支持以下格式指定, 其他需求请自行建表

          - 结尾为'\_date' : datetime
          - 结尾为'\_str' : string
          - 结尾为'\_num' : double
   5. rows :  当文档类型为二维表格时为必填项,  需要获取的表格内容范围,   当columns为列名映射时需要包含列名行,   表格类型为多维表格时不填
   6. table\_database : 选填项,  Doris的数据库名, 不填默认为小写ods, 区分大小写
   7. table\_module : 选填项,  Doris表模型, 不填默认为Duplicate
   8. table\_replace : 选填项,  默认0,  不对表做操作,  1或者True时,  每次执行会先删除表,  在字段变更后第一次执行改为1可改变表结构,  没有该表时不填或设为0,否则报错
   9. table\_bucket : 选填项,  默认3,  Doris分桶数量
   10. batch\_size: 选填项, 分批获取行数,  默认500行, 多维表格最大单次获取500行, 列数少时可以设大, 列数多或接口超时是设小
3. 注意:

   抽完之后核对数据,  部分字段可能因匹配不上或该字段类型获取不到且未报错,  可能会出现漏字段的情况
4. 更新说明:

   新增防呆,  用户经常修改飞书表结构导致报错,  防止用户加减列导致任务报错,  加减列导致报错的原因基本是因为找不到列,  或者列错位取不到,   扩大表格范围确保能取到所有列(如实际数据到AB列,  设定到AZ列,  用户在中间插入多列也不会报错),   根据列名写入到Doris,   飞书上面新增或删除的列不会再报错,   但是对应不上的字段数据会缺失

	新增支持多维表格

		- 支持绝大部分数据类型( *'流程',  '单向关联', '双向关联' 无法实现,  '直属上级', '工号' 由于权限问题可能获取不到*)

		- 字段中子项内容通过 '   ' 拼接

# 二、**模版**

1. ## 多维表格

```Shell
#!/bin/bash

python3 FeishutoDoris/feishu.py \
  --table_database test_db  \
  --table_name bitable1 \
  --table_module unique \
  --url "https://xxx.feishu.cn/base/xxxxx?table=xxxxx" \
  --columns "自动编号：bianhao，查找引用:chazhaoyinyong，文本:wenben,人员:renyuan,单选:danxuan,日期:riqi_date,附件:fujian,多选 ：duoxuan,群组:qunzu,数字 :shuzi ,复选框:fuxuankuang,公式:gongshi,条码:tiaoma, 进度: jindu , 货币 : huobi, 评分:pingfen, 地理位置: diliweizhi, 电话号码: dianhuahaoma, 修改人:xiugairen, 创建人:chuangjianren, 创建时间:chuangjianshijian_date, 最后更新时间:gengxinshijian_date, 超链接:chaolianjie, 邮箱:youxiang,部门:bumen" 
  
```

2. ## 飞书表格

-  列名映射与多维表格相同,  认定rows设定范围的第一行作为飞书表格列名,  如下例子"A1:E8"为飞书表格的列名

```Shell
#!/bin/bash

python3 FeishutoDoris/feishu.py \
  --table_database "test_db"  \
  --table_name "feishusheet1" \
  --table_module "unique" \
  --url "https://xxx.feishu.cn/base/xxxxx?table=xxxxx" \
  --columns "字段一:zd1, 字段二：zd2, 字段三：zd3, 字段四：zd4444, 字段五：zd55555" \
  --rows "A1:E8"
```

- 在表格中指定一列作为Doris表的表名

```Shell
#!/bin/bash

python3 FeishutoDoris/feishu.py \
  --table_database test_db  \
  --table_name feishusheet \
  --table_replace 1 \
  --url "https://xxx.feishu.cn/base/xxxxx?table=xxxxx" \
  --columns "A2:DG2" \
  --rows "A6:DG10000"
```

日志出现以下则为成功:

```Shell
        {
            "TxnId": 82689960,
            "Label": "14e0e286-7450-4c80-aa46-9ce19f1895d1",
            "Comment": "",
            "TwoPhaseCommit": "false",
            "Status": "Success",
            "Message": "OK",
            "NumberTotalRows": 2,
            "NumberLoadedRows": 2,
            "NumberFilteredRows": 0,
            "NumberUnselectedRows": 0,
            "LoadBytes": 636,
            "LoadTimeMs": 112,
            "BeginTxnTimeMs": 0,
            "StreamLoadPutTimeMs": 10,
            "ReadDataTimeMs": 0,
            "WriteDataTimeMs": 56,
            "CommitAndPublishTimeMs": 33
        }
```

# 三、常见错误

存在问题:

- 二维表格只有数值和文本两种类型,  自动建表时,  如果某个字段当时只有数字, 建表默认设该字段类型为数值,  如果下一次用户在这个字段填了文本会报错

# 四、权限

多维表格开了高级权限, 抽取报错或者抽取为空时, 需要给应用授权管理权限。
