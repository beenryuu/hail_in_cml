!pip3 install hail==0.2.50

!ls /home/cdsw/.local/lib/python3.6/site-packages/hail/backend

import os
os.environ['HAIL_HOME'] = '/home/cdsw/.local/lib/python3.6/site-packages/hail/backend'

hail_data = './data/'
hail_data

from pyspark.sql import SparkSession
from hail import *
import hail as hl
hl.init()

from hail.plot import show
from pprint import pprint
hl.plot.output_notebook()
hl.utils.get_1kg(hail_data)
hl.import_vcf(hail_data + '1kg.vcf.bgz').write(hail_data + "/1kg.mt", overwrite=True)

mt = hl.read_matrix_table(hail_data + "/1kg.mt")
mt.rows().select().show(5)
mt.row_key.show(5)
mt.s.show(5)
mt.entry.take(5)


table = (hl.import_table(hail_data + "/1kg_annotations.txt", impute=True).key_by('Sample'))
table.describe()
table.show(width=100)

print(mt.col.dtype)

mt = mt.annotate_cols(pheno = table[mt.s])
mt.col.describe()

hl.stop()
