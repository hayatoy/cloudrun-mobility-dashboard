from google.cloud import firestore
import pandas as pd
import numpy as np
import time


db = firestore.Client()

# Load dummy data
TRIPS_LAYER_DATA = 'https://raw.githubusercontent.com/visgl/deck.gl-data/master/examples/trips/trips-v7.json'
df = pd.read_json(TRIPS_LAYER_DATA)

vehicle_list = [6, 25, 77, 112, 114, 162, 185, 207, 211, 266, 269, 282, 287, 289, 304, 330, 353, 354, 364, 370, 381, 383, 390, 397, 409, 413, 432, 435, 467, 470]#, 471, 472, 475, 489, 493, 496, 499, 506, 511, 516, 523, 527, 529, 531, 547, 551, 556, 559, 561, 562, 571, 575, 578, 582, 583, 595, 608, 613, 615, 622, 626, 631, 632, 643, 645, 670, 689, 700, 701, 702, 728, 731, 739, 741, 756, 758, 777, 812, 817, 818, 820, 822, 826, 827, 829, 836, 870, 873, 878, 887, 888, 899, 900, 905, 912, 913, 916, 920, 934, 937, 938, 950, 955, 971, 974, 981, 985, 986, 991]

col_ref = db.collection('connected')
batch = db.batch()

try:
  while True:
    for i in range(70):
      for vehicle_ind in vehicle_list:
        _path = df.iloc[vehicle_ind]['path']
        print(len(_path))
        if i < len(_path):
          doc_ref = col_ref.document(str(vehicle_ind))
          batch.set(doc_ref,{
            'lonlat': _path[i],
            'timestamp': i
          })
      batch.commit()
except KeyboardInterrupt:
  pass
