import concurrent.futures
import os
import requests
import numpy as np
import math
import random
import string


class WMSDef:

    def __init__(self, endpoint, bounds, gsd, headers):
        self.endpoint = endpoint
        self.bounds = bounds
        self.gsd = gsd
        self.headers = headers


class WMSDownloader:
    """
    Given a WMS endpoint and WMS bounds and tile Sizes, calculaes the wms map requests needed and executes them
    """

    def __init__(self, wms_def, bounds=None, tile_size=(2048, 2048), output_folder=None):
        self.wms_def = wms_def
        self.bounds = bounds or wms_def.bounds
        self.tile_size = tile_size
        self.image_type = "png"

        if not output_folder:
            folder_name = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            output_folder = folder_path = "/tmp/" + folder_name
            os.mkdir(folder_path)
        self.output_folder = output_folder

    def execute(self):
        for (col, row), bbox in tileIterator(self.bounds, self.wms_def.gsd, self.tile_size):
            persister = self._get_persister(self.output_folder, "wms", col, row)
            bbox = "{},{},{},{}".format(*bbox)
            url = self.wms_def.endpoint.format(bbox=bbox, width=self.tile_size[0], height=self.tile_size[1])
            rq = HttRequest(url, persister)
            rq.exec(self.wms_def.params, self.wms_def.headers)

    def _get_persister(self, folder, prefix, col, row):
        file_path = os.path.join(folder, "{}_{}_{}.{}".format(prefix, col, row, self.image_type))
        return ResultPersister(file_path)


class HttRequest:
    """
    Executes a http request against an endpoint
    """

    def __init__(self, endpoint, persister=None):
        self.endpoint = endpoint
        self.persister = persister

    def exec(self, parameters={}, headers={}):
        url = self.endpoint.format(**parameters)

        response = requests.get(url, headers=headers)
        response.raise_for_status()  # ensure we notice bad responses

        if self.persister:
            # TODO: check support
            self.persister.store(response.content)
        return response


class ResultPersister:
    def __init__(self, output_file):
        self.output_file = output_file

    def store(self, content):
        with open(self.output_file, "wb") as file:
            file.write(content)


EARTH_RADIUS = 6378


def add_meters_to_latlong(latitude, longitude, dx, dy):
    """
    https://stackoverflow.com/questions/7477003/calculating-new-longitude-latitude-from-old-n-meters
    :param latitude:
    :param longitude:
    :param dx:
    :param dy:
    :return:
    """
    new_latitude = latitude + (dy / EARTH_RADIUS) * (180 / math.pi)
    new_longitude = longitude + (dx / EARTH_RADIUS) * (180 / math.pi) / math.cos(latitude * math.pi / 180)
    return new_latitude, new_longitude


def tileIterator(bounds, gsd, units="degrees", tile_size=(1024, 1024)):
    """
    Calculates iteration over an area with a given tile size
    bounds = xmin, ymax, xmax, ymin
    """

    min_x = bounds[0]
    min_y = bounds[1]
    max_x = bounds[2]
    max_y = bounds[3]

    # calculate the distance in the original coordinate system using as base the GSD of the WMS and the tile_size
    dx = round(tile_size[0] * gsd)
    dy = round(tile_size[1] * gsd)
    # project the distance in original coordinate system

    x_idx = 0
    y_idx = 0
    y = min_y
    while y < max_y:
        x = min_x
        while x < max_x:
            if units == "meters":
                width = round(tile_size[0] * gsd)
                height = round(tile_size[1] * gsd)
            else:
                add_meters_to_latlong(y, x, dx, dy)

            yield (x_idx, y_idx), (x, y, x + width, y + height)
            x_idx += 1
            if units == "degrees":
                _, x = add_meters_to_latlong(y, x, dx, dy)
            else:  # meters
                x += dx
        y_idx += 1
        if units == "degrees":
            y, _ = add_meters_to_latlong(y, x, dx, dy)
        else:  # meters
            y += dy


if __name__ == '__main__':

    # endpoint = "https://view.geoapi-airbusds.com/api/v1/map/imagery.wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&BBOX={x0},{y0},{x1},{y1}&SRS=EPSG:4326&WIDTH={width}&HEIGHT={height}&LAYERS=0&STYLES=&FORMAT=image/png&DPI=96&MAP_RESOLUTION=96&FORMAT_OPTIONS=dpi:96&TRANSPARENT=TRUE"
    # url = endpoint.format(x0="-7.27742081709148", y0=39.937068615427606, x1=-1.5995323079085204, y1=43.29125148744676,
    #                       height=851, width=740)
    endpoint = "https://view.geoapi-airbusds.com/api/v1/map/imagery.wms?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetMap&SRS=EPSG:4326&WIDTH={width}&HEIGHT={height}&LAYERS=0&STYLES=&FORMAT=image/png&DPI=96&MAP_RESOLUTION=96&FORMAT_OPTIONS=dpi:96&TRANSPARENT=TRUE"
    headers = {
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZDQyZGVlYy1jOTExLTQ3ODYtYmFiMy0wNDBmMTJlMzBiY2YiLCJhdWQiOiJodHRwczovL29uZWF0bGFzLmRhdGFkb29ycy5uZXQvaWQvcmVzb3VyY2VzIiwiaWRwIjoiVUNBX0FERlMiLCJhdXRoX3RpbWUiOjE2NTQyNzE3NTMsImFtciI6ImV4dGVybmFsIiwic2NvcGUiOlsib3BlbmlkIiwicHJvZmlsZSIsImVtYWlsIiwicm9sZXMiLCJyZWFkIiwid3JpdGUiXSwiaXNzIjoiaHR0cHM6Ly9vbmVhdGxhcy5kYXRhZG9vcnMubmV0L2lkIiwiZXhwIjoxNjg1ODk0MTUzLCJjbGllbnRfaWQiOiJwdGVzdF9leGFtcGxlXzEifQ.HW_xX1t0UeZcNEKIge8QvIgOqkZNzOBlSl8kru76NFjGQHfSo65wcmCPiyK8nCLKskZPF133p6mpXGqAotYs0nKXNwDwXCkwOTguoa2g-4QG-20eJ0aA8CbJlOZHFNtx7hJihkqMApvgANbqMMcZaPQ-If1HasUaffPKB2jIXRdvHI0MRj2dviBDD8gZN4EeLoJJ0q0DToXCVEnsJ9Ohm3xbZcyqMt5kic4AbPcEGX1xXXSGS7167GyuzxeAVtzTWuCR2aD-NWGkCy-FHzKqeazbr0zRFtqi440rdJyRzlzljn5TyeRXlqBq17YeBDOqLat41VdqE_ZzUkxsk4mo0A"
    }
    bounds = (-7.0891690350545442, 40.0735447153801161, -1.7633438829268657, 43.2475929997431123)

    wms_def = WMSDef(endpoint, bounds=bounds, gsd = 1.5, headers=headers)
    downloader = WMSDownloader(wms_def, bounds)
    downloader.execute()
