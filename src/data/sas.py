"Suggest a station"


import random
import time

import geopandas as gpd
import pandas as pd
import requests
from bs4 import BeautifulSoup


class SuggestAStation:
    def __init__(self):
        self.url = (
            "https://nycdotprojects.info/project-feedback-map/suggest-station-infill"
        )
        self.ajax = "https://nycdotprojects.info/views/ajax?_wrapper_format=drupal_ajax"

        # get initial page state
        base_page = requests.get(self.url)
        base_page.raise_for_status()
        soup = BeautifulSoup(base_page.text, "html.parser")
        self.max_comments = int(soup.find_all("span", "comments-count")[0].text)
        # comments intially loaded on page
        self.comments = [
            self._extract_comment(c)
            for c in soup.find_all("article", "approved-comment")
        ]
        self.remaining_comments = self.max_comments - len(self.comments)

    def _extract_comment(self, comment):
        resp = dict(
            id=comment["id"],
            category_id=comment["data-comment-category-id"],
            lon=comment["data-comment-lng"],
            lat=comment["data-comment-lat"],
            user=comment["data-comment-user-id"],
            description=comment.find("p").text,
        )

        # not always present
        try:
            resp["location"] = comment["data-comment-locsumm"]
        except KeyError:
            resp["location"] = None

        # sometimes empty coordinates ''
        try:
            resp["lon"] = float(resp["lon"])
            resp["lat"] = float(resp["lat"])
        except ValueError:
            resp["lon"] = None
            resp["lat"] = None

        return resp

    def get_comments(self, page):
        payload = f"view_name=mapcomments&view_display_id=block_1&view_args=1076&view_path=%2Fnode%2F1076&field_map_comment_category_target_id=All&page={page}"
        headers = {
            "authority": "nycdotprojects.info",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://nycdotprojects.info",
            "referer": self.url,
        }

        response = requests.request("POST", self.ajax, headers=headers, data=payload)
        response.raise_for_status()
        data = list(
            filter(
                lambda x: x["command"] == "insert"
                and x["method"] == "infiniteScrollInsertView",
                response.json(),
            )
        )[0]["data"]
        soup = BeautifulSoup(data, "html.parser")
        comment_soup = soup.find_all("article", "approved-comment")
        comments = [self._extract_comment(com) for com in comment_soup]

        return comments

    def add_remaining_comments(self, delay_requests=True):
        remaining = True
        page = 1
        while remaining:
            if delay_requests:
                time.sleep(1 + random.random())
            new_comments = self.get_comments(page)
            if len(new_comments) == 0:
                remaining = False
            else:
                self.comments += new_comments
            page += 1

    def gdf(self, to_crs="EPSG:2263"):
        df = pd.DataFrame.from_records(self.comments)
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.lon, df.lat), crs=4326
        )
        gdf.to_crs(to_crs, inplace=True)
        return gdf

    def process(self, output_file, to_crs="EPSG:2263", delay_requests=True):
        self.add_remaining_comments(delay_requests=delay_requests)
        gdf = self.gdf(to_crs=to_crs)
        gdf.to_file(output_file, crs=to_crs)
