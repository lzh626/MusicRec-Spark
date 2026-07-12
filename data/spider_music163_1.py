
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os

# ================= 配置区域 =================
# 分块爬取，每块10页，总共爬 3 块 = 30页
#修改 TOTAL_CHUNKS = 1，运行 python3 spider_music163_final_v2.py。
TOTAL_CHUNKS = 5
PAGES_PER_CHUNK = 10

# 更丰富的 User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

FILE_META = 'processed/tracks_meta_final.csv'
FILE_INTERACT = 'processed/interactions_final.csv'
# ===========================================

def get_playlist_ids_from_page(page_num):
    offset = page_num * 35
    url = f'https://music.163.com/discover/playlist/?order=hot&cat=全部&limit=35&offset={offset}'
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS), 'Referer': 'https://music.163.com/'}
        r = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        playlist_links = soup.find_all('a', {'class': 'msk'})
        ids = [link.get('href').split('=')[1] for link in playlist_links if link.get('href').startswith('/playlist?id=')]
        return ids
    except:
        return []

def get_playlist_songs_api(playlist_id):
    url = f'http://music.163.com/api/playlist/detail?id={playlist_id}'
    try:
        # 更长的随机延时
        time.sleep(random.uniform(0.8, 2.0))
        headers = {'User-Agent': random.choice(USER_AGENTS), 'Referer': 'https://music.163.com/'}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        
        if 'result' in data and 'tracks' in data['result']:
            return [{
                'song_id': song['id'],
                'title': song.get('name', 'Unknown'),
                'artist': song.get('artists', [{}])[0].get('name', 'Unknown'),
                'image_url': song.get('album', {}).get('picUrl', ''),
                'playlist_id': playlist_id
            } for song in data['result']['tracks']]
    except:
        return []

# ================= 主程序 =================
all_songs = []
start_page = 0

# 检查是否有之前爬取的中断文件
temp_file = 'processed/temp_songs.csv'
if os.path.exists(temp_file):
    print("发现上次中断的进度，正在加载...")
    df_temp = pd.read_csv(temp_file)
    all_songs = df_temp.to_dict('records')
    last_page_crawled = df_temp['page_crawled'].max()
    start_page = int(last_page_crawled) + 1
    print(f"从第 {start_page + 1} 页继续...")

total_pages = TOTAL_CHUNKS * PAGES_PER_CHUNK

try:
    for page in range(start_page, total_pages):
        print(f"\n--- 正在处理第 {page + 1} / {total_pages} 页 ---")
        pids = get_playlist_ids_from_page(page)
        
        if len(pids) < 30: # 如果一页少于30个歌单，说明被限制了
            print(f"⚠️ 第 {page + 1} 页仅发现 {len(pids)} 个歌单，可能已被限制。暂停 60 秒...")
            time.sleep(60)
        
        for pid in pids:
            # 【新增修复】如果 songs 是 None 或者空列表，直接跳过
            songs = get_playlist_songs_api(pid)
            if not songs:
            	continue

            # 记录当前爬取页码
            for song in songs:
                song['page_crawled'] = page
            all_songs.extend(songs)
            
            if len(all_songs) % 500 < 50:
                print(f"当前已捕获: {len(all_songs)} 首")
        
        # 每爬 10 页保存一次进度
        if (page + 1) % 10 == 0:
            print(f"\n💾 完成第 {page + 1} 页，正在保存临时进度...")
            pd.DataFrame(all_songs).to_csv(temp_file, index=False)

except KeyboardInterrupt:
    print("\n⚠️ 用户手动停止！正在保存已抓取的数据...")

# ================= 数据清洗与保存 =================
print("\n🧹 正在去重和清洗数据...")
df_songs = pd.DataFrame(all_songs)

if not df_songs.empty:
    before = len(df_songs)
    df_songs = df_songs.drop_duplicates(subset=['song_id'])
    after = len(df_songs)
    print(f"📊 歌曲去重: {before} -> {after} 首")
    
    # ... (省略生成交互数据的代码，和之前一样)
    print("🎲 生成用户交互数据...")
    user_interactions = []
    song_ids = df_songs['song_id'].tolist()
    for user_id in range(1, 2001):
        num_plays = random.randint(30, 100)
        if len(song_ids) > num_plays:
            played = random.sample(song_ids, num_plays)
            for sid in played:
                user_interactions.append({'user_id': user_id, 'song_id': sid, 'playcount': random.randint(1, 200)})
    df_inter = pd.DataFrame(user_interactions)
    
    # 保存最终结果
    os.makedirs('processed', exist_ok=True)
    df_songs.to_csv(FILE_META, index=False)
    df_inter.to_csv(FILE_INTERACT, index=False)
    
    print(f"\n✅ 成功！文件已保存：")
    print(f"   1. {FILE_META} (歌曲数: {len(df_songs)})")
    print(f"   2. {FILE_INTERACT} (交互数: {len(df_inter)})")
    
    # 删除临时文件
    if os.path.exists(temp_file):
        os.remove(temp_file)
else:
    print("❌ 失败，没抓到数据")
