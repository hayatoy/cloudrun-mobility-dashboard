from google.cloud import firestore
import pandas as pd
import numpy as np
import time


db = firestore.Client()

# Load dummy data
TRIPS_LAYER_DATA = 'https://raw.githubusercontent.com/visgl/deck.gl-data/master/examples/trips/trips-v7.json'
df = pd.read_json(TRIPS_LAYER_DATA)

new_data = []
for ind, row in df.iterrows():
  ts_list = row['timestamps']
  lonlat_list = np.array(row['path'])
  ts_range = np.arange(int(ts_list[0]), int(ts_list[-1]))
  lon = np.interp(ts_range, ts_list, lonlat_list[:, 0])
  lat = np.interp(ts_range, ts_list, lonlat_list[:, 1])
  lonlat_new = np.concatenate([lon.reshape(-1,1), lat.reshape(-1,1)], axis=1)
  for lonlat, ts in zip(lonlat_new, ts_range):
    new_data.append({'vehicle_id': ind,
                       'lonlat': lonlat,
                       'ts': ts})
df_flat = pd.DataFrame(new_data)
df_flat_sort = df_flat.sort_values(by='ts')
# df_flat_sort['ts'] = df_flat_sort['ts'] // 10
vehicle_list = [6, 25, 26, 35, 46, 49, 55, 57, 58, 62, 66, 69, 77, 89, 98, 112, 114, 115, 117, 134, 142, 151, 162, 173, 175, 185, 196, 197, 204, 207, 210, 211, 219, 228, 229, 231, 240, 246, 250, 251, 252, 256, 258, 261, 264, 266, 269, 277, 280, 282, 287, 289, 298, 299, 302, 304, 308, 310, 311, 318, 321, 322, 324, 327, 328, 330, 331, 332, 337, 338, 344, 347, 353, 354, 356, 364, 369, 370, 371, 380, 381, 383, 390, 396, 397, 404, 405, 409, 411, 413, 418, 425, 429, 432, 433, 434, 435, 437, 438, 439, 451, 454, 462, 467, 469, 470, 471, 472, 475, 477, 479, 481, 486, 487, 489, 491, 493, 495, 496, 498, 499, 501, 506, 508, 511, 515, 516, 523, 525, 526, 527, 529, 530, 531, 532, 536, 537, 540, 541, 543, 544, 545, 547, 550, 551, 554, 555, 556, 559, 560, 561, 562, 564, 566, 569, 571, 574, 575, 578, 579, 580, 582, 583, 586, 591, 593, 595, 599, 602, 605, 608, 610, 613, 614, 615, 618, 619, 621, 622, 626, 631, 632, 634, 635, 640, 642, 643, 645, 647, 652, 654, 657, 659, 662, 664, 665, 668, 669, 670, 671, 674, 676, 682, 683, 686, 689, 695, 696, 697, 700, 701, 702, 718, 721, 728, 731, 737, 738, 739, 741, 743, 744, 746, 749, 756, 758, 760, 762, 764, 766, 770, 773, 774, 776, 777, 778, 779, 781, 782, 783, 789, 790, 795, 799, 801, 809, 812, 813, 814, 816, 817, 818, 820, 822, 826, 827, 828, 829, 830, 831, 834, 835, 836, 837, 841, 846, 853, 858, 866, 870, 871, 873, 875, 877, 878, 879, 887, 888, 889, 891, 892, 894, 895, 896, 898, 899, 900, 901, 905, 910, 911, 912, 913, 914, 915, 916, 917, 919, 920, 925, 926, 927, 930, 933, 934, 937, 938, 942, 944, 946, 950, 955, 965, 969, 970, 971, 972, 974, 979, 981, 982, 985, 986, 987, 988, 990, 991, 992]
vehicle_list = vehicle_list[:100]
df = df_flat_sort[df_flat_sort['vehicle_id'].isin(vehicle_list)]

col_ref = db.collection('connected')
batch = db.batch()

doc_ref = col_ref.document('0')
doc_ref.set({
        'lonlat': [0,0],
        'timestamp': 0
        })

try:
  while True:
    for ts, group in df.groupby(by='ts'):
      print(ts)
      for ind, row in group.iterrows():
        doc_ref = col_ref.document(str(row['vehicle_id']))
        batch.set(doc_ref,{
        'lonlat': list(row['lonlat']),
        'timestamp': int(row['ts'])
        })
      batch.commit()
except KeyboardInterrupt:
  pass
