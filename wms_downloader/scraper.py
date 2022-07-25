import concurrent.futures
import itertools
import os
import random
import shutil
import string
from io import BytesIO

import numpy as np
import requests
from matplotlib import image

# # endpoint = "https://view.geoapi-airbusds.com/api/v1/map/imagery.wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&BBOX=-7.27742081709148,39.937068615427606,-1.5995323079085204,43.29125148744676&SRS=EPSG:4326&WIDTH=740&HEIGHT=851&LAYERS=0&STYLES=&FORMAT=image/png&DPI=96&MAP_RESOLUTION=96&FORMAT_OPTIONS=dpi:96&TRANSPARENT=TRUE"
# endpoint = "https://view.geoapi-airbusds.com/api/v1/map/imagery.wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&BBOX={x0},{y0},{x1},{y1}&SRS=EPSG:4326&WIDTH={width}&HEIGHT={height}&LAYERS=0&STYLES=&FORMAT=image/png&DPI=96&MAP_RESOLUTION=96&FORMAT_OPTIONS=dpi:96&TRANSPARENT=TRUE"
# url = endpoint.format(x0="-7.27742081709148", y0=39.937068615427606, x1=-1.5995323079085204, y1=43.29125148744676,
#                       height=851, width=740)

endpoint = "https://view.geoapi-airbusds.com/api/v1/map/imagery.wmts?layer=OneLive&style=&tilematrixset=3857&Service=WMTS&Request=GetTile&Version=1.0.0&Format=image%2Fjpeg&TileMatrix={level}&TileRow={row}&TileCol={col}"
headers = {
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZDQyZGVlYy1jOTExLTQ3ODYtYmFiMy0wNDBmMTJlMzBiY2YiLCJhdWQiOiJodHRwczovL29uZWF0bGFzLmRhdGFkb29ycy5uZXQvaWQvcmVzb3VyY2VzIiwiaWRwIjoiVUNBX0FERlMiLCJhdXRoX3RpbWUiOjE2NTQyNzE3NTMsImFtciI6ImV4dGVybmFsIiwic2NvcGUiOlsib3BlbmlkIiwicHJvZmlsZSIsImVtYWlsIiwicm9sZXMiLCJyZWFkIiwid3JpdGUiXSwiaXNzIjoiaHR0cHM6Ly9vbmVhdGxhcy5kYXRhZG9vcnMubmV0L2lkIiwiZXhwIjoxNjg1ODk0MTUzLCJjbGllbnRfaWQiOiJwdGVzdF9leGFtcGxlXzEifQ.HW_xX1t0UeZcNEKIge8QvIgOqkZNzOBlSl8kru76NFjGQHfSo65wcmCPiyK8nCLKskZPF133p6mpXGqAotYs0nKXNwDwXCkwOTguoa2g-4QG-20eJ0aA8CbJlOZHFNtx7hJihkqMApvgANbqMMcZaPQ-If1HasUaffPKB2jIXRdvHI0MRj2dviBDD8gZN4EeLoJJ0q0DToXCVEnsJ9Ohm3xbZcyqMt5kic4AbPcEGX1xXXSGS7167GyuzxeAVtzTWuCR2aD-NWGkCy-FHzKqeazbr0zRFtqi440rdJyRzlzljn5TyeRXlqBq17YeBDOqLat41VdqE_ZzUkxsk4mo0A"
}


def down(level, row, col, output=None):
    url = endpoint.format(level=level, row=row, col=col)
    # print(url)
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # ensure we notice bad responses
    if output is not None:
        with open(output, "wb") as file:
            file.write(response.content)
    else:
        buffer = BytesIO(response.content)
        img = image.imread(buffer)
        return img


def generate_worker_tasks(level, row, col, shared_image, tile_size=256):
    def myf(index):
        idx_row, idx_col = index
        img = down(level, row + idx_row, col + idx_col)
        shared_image[idx_row * tile_size:(idx_row + 1) * tile_size, idx_col * tile_size:(idx_col + 1) * tile_size,:] = (img[:, :, 0:3] * 255).astype(np.uint8)
        if idx_row * idx_col % 100 == 0:
            print("Downloading image: {}".format(idx_row * idx_col))

    return myf


# max = 25
# files = []
# for i in range(0, max):
#     f = "/tmp/salida_{}.jpeg".format(i)
#     files.append(f)
#     down(22, row=1555986, col=2039196 + i, output=f)
#
#     # 3204 GET https://view.geoapi-airbusds.com/mugg/wmts/4d5ce8a2-cc56-11ec-a266-af2e93a13124/tile/1.0.0/18306ab0-08dc-11ed-9bd9-42010ad101c4/default/3857/22/1555986/2039196
#     imgs = [image.imread(x) for x in files]
#     im = np.hstack(imgs)
#     image.imsave("/tmp/salida_final.jpeg", im)

if __name__ == '__main__':
    import time

    # create output_folder
    folder_name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
    output_folder = folder_path = "/tmp/" + folder_name
    output_folder = "/tmp/test1"
    shutil.rmtree(output_folder, ignore_errors=True)
    os.mkdir(output_folder)

    start = time.perf_counter()

    tile_size = 256
    NUM_COLS = 50
    NUM_ROWS = 50
    shared_img = np.zeros((NUM_ROWS * tile_size, NUM_COLS * tile_size, 3), np.uint8)

    start = time.perf_counter()

    worker_task = generate_worker_tasks(level=22, row=1555986, col=2039196, shared_image=shared_img)

    with concurrent.futures.ThreadPoolExecutor(20) as exe:
        exe.map(worker_task, itertools.product(range(NUM_ROWS), range(NUM_COLS)))

    image.imsave("/tmp/salida_final.png", shared_img)
    print("Image size: {}".format(shared_img.shape))

    finish = time.perf_counter()
    print(f'It took {finish - start: .2f} second(s) to finish')
