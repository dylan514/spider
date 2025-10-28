# encoding:utf-8
"""
微博爬虫工具
功能：根据配置参数自动爬取微博内容及评论，支持批量运行
"""
import requests
import re
import json
import csv
import time
import random
import logging
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import math
from typing import List, Dict, Optional

# 配置日志系统
#这里也略微改了一下，换成了绝对地址
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('E:\crawler\log\weibo_spider.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# User-Agent池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/113.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
]

# 爬虫配置参数（在此处修改配置）
CONFIG = {
    'cookie': 'XSRF-TOKEN=ChlOYJ1NH2fNvL3Td5cz12xd; SUB=_2A25EBBS1DeRhGeFG6lEY9SbNyziIHXVneCh9rDV8PUNbmtANLW_tkW9NflXNhZnBmmnFT3zmBE7r4lylgctgcvZd; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhPpzBJCxAH3od7M7_nabWn5NHD95QN1h201K-ReK5XWs4DqcjMi--NiK.Xi-2Ri--ciKnRi-zNS0npe0.f1h27S5tt; WBPSESS=AXdRp_yg2JnACNVAkGO9_x-q0YKtJMYo8cvZpD5gJetBKEIFnbSnuzTUxIDePJjqMlYJ7Oi-FYP-a8hwj75onSVLygu1F_z9OYbrx4PlRKdp4Ec-1nz3DlC43292onbU-lLT4GSQsniMwYDVpjsPag==; SCF=AtCupK-XqRBruniZ3RzeUQv3g3iCfrHIs0rvLzvhEbAvy_9coungSfFcKbmSntXzxirW-6hZiValGiu2ABwKVfY.; ALF=02_1764225509',  # 请填入你的微博Cookie
    #cookie值，用于登录认证
    'keyword': '对立',  # 搜索关键词
    'from_time': '2025-10-24 00:00:00',  # 开始时间
    'to_time': '2025-10-25 00:00:00',  # 结束时间
    'frequency': 3600,  # 时间区间间隔（秒），默认1小时
    'crawl_comments': False  # 是否爬取评论
}


class WbTool:
    """微博数据处理工具类"""

    def __init__(self):
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.processed_mids = set()  # 用于去重

    def extract_uid(self, url: str) -> str:
        """提取用户UID"""
        try:
            return str(url).split('/')[-1].split('?')[0]
        except Exception as e:
            logger.warning(f"提取UID失败，URL: {url}，错误: {e}")
            return ""

    def clean_content(self, text: str) -> str:
        """清洗微博内容"""
        if not text:
            return ""
        content = re.sub(r'[\n\s\u200b\ue627]', '', str(text))
        content = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9，。,.;;！!？?：:]', '', content)
        return content

    def text_cleaning(self, text: str) -> str:
        """通用文本清洗"""
        return str(text).replace('\n', '').replace(' ', '') if text else ""

    def process_interaction_data(self, text: str) -> int:
        """处理互动数据"""
        if not text:
            return 0
        text = re.sub(r'[^\d]', '', str(text).strip())
        return int(text) if text else 0

    def parse_weibo_time(self, timestr: str) -> str:
        """解析微博时间"""
        if not timestr:
            return ""
        timestr = str(timestr).split('转赞人数超过')[0].strip()

        # 处理分钟前、秒前
        if '分' in timestr:
            try:
                minutes = int(re.search(r'(\d+)分', timestr).group(1))
                past_time = datetime.now() - timedelta(minutes=minutes)
                return past_time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                logger.warning(f"解析分钟时间失败: {timestr}，错误: {e}")

        if '秒' in timestr:
            try:
                seconds = int(re.search(r'(\d+)秒', timestr).group(1))
                past_time = datetime.now() - timedelta(seconds=seconds)
                return past_time.strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                logger.warning(f"解析秒时间失败: {timestr}，错误: {e}")

        # 处理今天
        if '今天' in timestr:
            time_part = timestr.replace('今天', '').strip()
            return f"{self.today} {time_part}:00" if time_part else f"{self.today} 00:00:00"

        # 处理月/日格式
        if '月' in timestr and '年' not in timestr:
            try:
                match = re.search(r'(\d+)月(\d+)日(?: (\d+:\d+))?', timestr)
                if match:
                    month, day = int(match.group(1)), int(match.group(2))
                    time_part = match.group(3) or "00:00"
                    current_year = datetime.now().year
                    if month > datetime.now().month:
                        current_year -= 1
                    return f"{current_year}-{month:02d}-{day:02d} {time_part}:00"
            except Exception as e:
                logger.warning(f"解析月日时间失败: {timestr}，错误: {e}")

        # 处理完整年月日格式
        if '年' in timestr and '月' in timestr and '日' in timestr:
            try:
                return re.sub(r'年|月', '-', timestr.replace('日', '')) + ":00"
            except Exception as e:
                logger.warning(f"解析完整时间失败: {timestr}，错误: {e}")

        return timestr

    def get_time_ranges(self, from_time: str, to_time: str, frequency: int) -> List[List[str]]:
        """生成时间区间"""
        try:
            from_dt = datetime.strptime(from_time, '%Y-%m-%d %H:%M:%S')
            to_dt = datetime.strptime(to_time, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.error(f"时间格式错误，from: {from_time}, to: {to_time}，错误: {e}")
            return []

        time_ranges = []
        current = from_dt
        while current < to_dt:
            next_time = current + timedelta(seconds=frequency)
            if next_time > to_dt:
                next_time = to_dt
            time_ranges.append([
                current.strftime('%Y-%m-%d %H:%M:%S'),
                next_time.strftime('%Y-%m-%d %H:%M:%S')
            ])
            current = next_time
        return time_ranges

    def format_time_for_url(self, t: str) -> str:
        """格式化时间为URL参数"""
        try:
            dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
            return f"{dt.strftime('%Y-%m-%d')}-{dt.hour}"
        except Exception as e:
            logger.warning(f"时间格式转换失败: {t}，错误: {e}")
            return ""

    # def save_to_csv(self, filename: str, content: List) -> None:
    #     """保存到CSV"""
    #     try:
    #         with open(f'{filename}.csv', 'a+', newline='', encoding='utf-8-sig') as fp:
    #             csv.writer(fp).writerow(content)
    #     except Exception as e:
    #         logger.error(f"写入CSV失败，文件: {filename}，错误: {e}")
    ##这里改了一下，io类型的不要存在当前目录下

    def save_to_csv(self, filename: str, content: List) -> None:
        """保存到CSV（使用绝对路径）"""
        try:
            # 直接使用绝对路径
            absolute_path = f'E:/crawler/{filename}.csv'
            with open(absolute_path, 'a+', newline='', encoding='utf-8-sig') as fp:
                csv.writer(fp).writerow(content)
        except Exception as e:
            logger.error(f"写入CSV失败，文件: {filename}，错误: {e}")


class WeiboSpider(WbTool):
    """微博爬虫类"""

    def __init__(self, config: Dict):
        super().__init__()
        self.config = config
        self.cookie = config['cookie']
        self.keyword = config['keyword']
        self.crawl_comments = config['crawl_comments']
        self.headers = self._build_headers()

    def _build_headers(self) -> Dict[str, str]:
        """构建请求头"""
        return {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'max-age=0',
            'cookie': self.cookie,
            'user-agent': random.choice(USER_AGENTS)
        }

    def _get_response(self, url: str, is_async: bool = False) -> Optional[requests.Response]:
        """发送请求并获取响应"""
        headers = self.headers.copy()
        if is_async:
            headers['accept'] = 'application/json, text/plain, */*'
            headers['x-requested-with'] = 'XMLHttpRequest'

        retry_count = 1
        for i in range(retry_count):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=10,
                    verify=False
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                #logger.warning(f"请求失败（{i + 1}/{retry_count}），URL: {url}，错误: {e}")
                if i < retry_count - 1:
                    time.sleep(2 ** i)
        #logger.error(f"请求多次失败，URL: {url}")
        return None

    def get_blogger_info(self, uid: str) -> List:
        """获取博主信息"""
        if not uid:
            return [""] * 12
        url = f'https://weibo.com/ajax/profile/info?uid={uid}'
        response = self._get_response(url, is_async=True)
        if not response:
            return [""] * 12

        try:
            jsondata = json.loads(response.text)
            if jsondata.get('ok') != 1:
                return [""] * 12

            user = jsondata['data']['user']
            gender = '男' if user.get('gender') == 'm' else '女'
            followers = user.get('followers_count', 0)
            public_media = '公共媒体' if followers > 10000 else 'nan'
            log10 = math.log10(followers) if followers > 0 else 'nan'

            verified = user.get('verified', False)
            member_type = '无认证'
            if verified:
                v_type = user.get('verified_type', -1)
                v_ext = user.get('verified_type_ext', -1)
                if v_type == 0 and v_ext == 1:
                    member_type = '微博个人认证(红V)'
                elif v_type == 3:
                    member_type = '微博官方认证(蓝V)'
                elif v_type == 0 and v_ext == 0:
                    member_type = '微博个人认证(黄V)'
                else:
                    member_type = '其他认证'

            return [
                user.get('screen_name', ''),
                gender,
                self.text_cleaning(user.get('description', '')),
                followers,
                user.get('friends_count', 0),
                user.get('location', ''),
                user.get('statuses_count', 0),
                verified,
                user.get('avatar_large', ''),
                public_media,
                log10,
                member_type
            ]
        except Exception as e:
            logger.error(f"解析博主信息失败，UID: {uid}，错误: {e}")
            return [""] * 12

    def crawl_comments(self, mid: str, uid: str) -> None:
        """爬取评论"""
        if not mid or not uid:
            return
        comment_filename = f"{self.keyword}_comments_{datetime.now().strftime('%Y%m%d')}"
        max_id = 0
        page = 0

        while True:
            params = {
                'is_asc': '0',
                'is_reload': '1',
                'id': mid,
                'is_show_bulletin': '1',
                'is_mix': '0',
                'max_id': max_id,
                'count': '20',
                'uid': uid
            }
            url = f'https://weibo.com/ajax/statuses/buildComments?{requests.compat.urlencode(params)}'
            response = self._get_response(url, is_async=True)
            if not response:
                break

            try:
                jsondata = json.loads(response.text)
                if jsondata.get('ok') != 1:
                    break

                comments = jsondata.get('data', [])
                total = jsondata.get('total_number', 0)
                if not comments:
                    logger.info(f"MID: {mid} 无更多评论（共{total}条）")
                    break

                for comment in comments:
                    user = comment.get('user', {})
                    data = [
                        mid, uid,
                        self._parse_comment_time(comment.get('created_at', '')),
                        self.clean_content(comment.get('text_raw', '')),
                        user.get('id', ''),
                        user.get('screen_name', ''),
                        self.text_cleaning(user.get('description', '')),
                        user.get('friends_count', 0),
                        user.get('followers_count', 0),
                        comment.get('like_counts', 0),
                        user.get('statuses_count', 0),
                        user.get('location', '')
                    ]
                    self.save_to_csv(comment_filename, data)

                max_id = jsondata.get('max_id', 0)
                if max_id == 0:
                    break

                page += 1
                logger.info(f"MID: {mid} 评论爬取中，第{page}页（共{total}条）")
                time.sleep(random.uniform(1, 3))

            except Exception as e:
                logger.error(f"解析评论失败，MID: {mid}，错误: {e}")
                break

    def _parse_comment_time(self, timestr: str) -> str:
        """解析评论时间"""
        try:
            dt = datetime.strptime(timestr, '%a %b %d %H:%M:%S %z %Y')
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.warning(f"解析评论时间失败: {timestr}，错误: {e}")
            return ""

    def parse_weibo_list(self, url: str) -> None:
        """解析微博列表"""
        response = self._get_response(url)
        if not response:
            return

        if '未找到' in response.text:
            logger.info("未找到相关微博内容")
            return

        html = BeautifulSoup(response.text, 'html.parser')
        feed_list = html.find('div', id="pl_feedlist_index")
        if not feed_list:
            logger.warning("未找到微博内容容器，可能Cookie失效")
            #如果报这个错了，很大可能上就是因为太频繁了所以把账号给封了
            return

        weibo_items = feed_list.find_all('div', class_="card-wrap")
        for item in weibo_items:
            if 'mid' not in item.attrs:
                continue

            try:
                mid = item['mid']
                if mid in self.processed_mids:
                    continue
                self.processed_mids.add(mid)

                # 发布者信息
                name_tag = item.find('a', class_="name")
                nickname = self.text_cleaning(name_tag.text) if name_tag else ""
                avatar_tag = item.find('div', class_="avator")
                home_link = "https:" + avatar_tag.find('a')['href'] if (avatar_tag and avatar_tag.find('a')) else ""
                uid = self.extract_uid(home_link)

                # 微博内容
                txt_tag = item.find_all('p', class_="txt")[-1] if item.find_all('p', class_="txt") else None
                content = self.clean_content(txt_tag.text) if txt_tag else ""

                # 发布时间和来源
                from_tag = item.find('p', class_="from") or item.find('div', class_="from")
                if not from_tag:
                    continue
                time_tag = from_tag.find('a', target="_blank")
                publish_time = self.parse_weibo_time(time_tag.text) if time_tag else ""
                weibo_link = "https:" + time_tag['href'] if time_tag else ""
                device_tag = from_tag.find('a', rel="nofollow")
                publish_device = self.text_cleaning(device_tag.text) if device_tag else "未知"

                # 互动数据
                act_tags = item.find('div', class_="card-act").find_all('li')
                act_tags = act_tags[1:] if len(act_tags) >= 4 else act_tags
                interactions = [self.process_interaction_data(tag.text) for tag in act_tags[:3]]
                repost, comment, like = (interactions + [0, 0, 0])[:3]

                # 博主信息
                blogger_info = self.get_blogger_info(uid)

                # 保存数据
                data = [
                           uid, mid, publish_time, content, weibo_link, nickname,
                           publish_device, home_link, repost, comment, like
                       ] + blogger_info
                self.save_to_csv(self._get_weibo_filename(), data)

                # 爬取评论
                if self.crawl_comments and comment > 0:
                    self.crawl_comments(mid, uid)
                    time.sleep(random.uniform(2, 5))

            except Exception as e:
                logger.error(f"解析微博失败，MID: {item.get('mid', '未知')}，错误: {e}")
                continue

        # 下一页处理
        next_page = html.find('a', class_="next")
        if next_page and 'href' in next_page.attrs:
            next_url = f'https://s.weibo.com{next_page["href"]}'
            logger.info(f"准备爬取下一页: {next_url}")
            time.sleep(random.uniform(3, 7))
            self.parse_weibo_list(next_url)

    def _get_weibo_filename(self) -> str:
        """生成微博文件名"""
        return f"{self.keyword}_weibo_{datetime.now().strftime('%Y%m%d')}"

    def _init_csv_files(self) -> None:
        """初始化CSV文件"""
        # 微博表头
        weibo_cols = [
            'UID', 'MID', '发布时间', '微博内容', '微博链接', '发布者昵称', '发布来源',
            '主页链接', '转发数', '评论数', '点赞数', '博主昵称', '性别', '个人签名',
            '粉丝数', '关注数', '所属IP地址', '微博数', '是否认证', '头像链接',
            '媒体类型', 'log10', '认证类型'
        ]
        self.save_to_csv(self._get_weibo_filename(), weibo_cols)

        # 评论表头
        if self.crawl_comments:
            comment_cols = [
                'MID', 'UID', '评论时间', '评论内容', '评论人UID', '评论人昵称',
                '评论人个性签名', '关注数量', '粉丝数量', '点赞数量', '作品数', 'IP所属'
            ]
            self.save_to_csv(f"{self.keyword}_comments_{datetime.now().strftime('%Y%m%d')}", comment_cols)

    def start_crawl(self) -> None:
        """开始爬取"""
        if not self.cookie:
            logger.error("请先填写Cookie配置")
            return

        time_ranges = self.get_time_ranges(
            self.config['from_time'],
            self.config['to_time'],
            self.config['frequency']
        )
        if not time_ranges:
            logger.error("生成时间区间失败")
            return

        self._init_csv_files()
        logger.info(f"开始爬取关键词: {self.keyword}，时间范围: {self.config['from_time']} 至 {self.config['to_time']}")

        for i, (start, end) in enumerate(time_ranges):
            logger.info(f"爬取时间区间 {i + 1}/{len(time_ranges)}: {start} ~ {end}")
            start_str = self.format_time_for_url(start)
            end_str = self.format_time_for_url(end)
            if not start_str or not end_str:
                continue

            encoded_keyword = requests.compat.quote(self.keyword)
            url = (f'https://s.weibo.com/weibo?q={encoded_keyword}&typeall=1&suball=1'
                   f'&timescope=custom:{start_str}:{end_str}&Refer=g')

            self.parse_weibo_list(url)

            # 区间爬取后休息
            if (i + 1) % 5 == 0:
                sleep_time = random.randint(10, 20)
                logger.info(f"已完成{i + 1}个区间，休息{sleep_time}秒")
                time.sleep(sleep_time)

        logger.info("爬取完成！")


def main():
    # 忽略SSL警告
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 初始化爬虫并开始爬取
    try:
        spider = WeiboSpider(CONFIG)
        spider.start_crawl()
    except Exception as e:
        logger.critical(f"爬虫运行失败，错误: {e}", exc_info=True)


if __name__ == '__main__':
    main()