import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os

# ================= 配置区域 =================
# 每次运行爬取多少页
PAGES_PER_RUN = 5 

# 状态文件：用于记录上次爬到了第几页
STATE_FILE = 'spider_state.txt'

# 输出文件路径
FILE_META = 'processed/netease/tracks_meta_final.csv'
FILE_INTERACT = 'processed/netease/interactions_final.csv'

# User-Agent 池
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
]
# ===========================================

def get_start_page():
    """读取上次爬取到的页码"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except:
                return 0
    return 0

def save_current_page(page_num):
    """保存当前进度"""
    with open(STATE_FILE, 'w') as f:
        f.write(str(page_num))

def get_playlist_ids_from_page(page_num):
    offset = page_num * 35
    url = f'https://music.163.com/discover/playlist/?order=hot&cat=全部&limit=35&offset={offset}'
    try:
        headers = {'User-Agent': random.choice(USER_AGENTS), 'Referer': 'https://music.163.com/'}
        r = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        playlist_links = soup.find_all('a', {'class': 'msk'})
        ids = [link.get('href').split('=')[1] for link in playlist_links if link.get('href').startswith('/playlist?id=')]
        return ids
    except:
        return []

def get_playlist_songs_api(playlist_id):
    url = f'http://music.163.com/api/playlist/detail?id={playlist_id}'
    try:
        time.sleep(random.uniform(0.5, 1.5)) # 随机延时
        headers = {'User-Agent': random.choice(USER_AGENTS), 'Referer': 'https://music.163.com/'}
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        
        if 'result' in data and 'tracks' in data['result']:
            return [{
                'song_id': song['id'],
                'title': song.get('name', 'Unknown'),
                'artist': song.get('artists', [{}])[0].get('name', 'Unknown'),
                'image_url': song.get('album', {}).get('picUrl', ''),
                'source': 'netease', # 【关键】手动打上来源标签，与 Spotify 区分
                'playlist_id': playlist_id
            } for song in data['result']['tracks']]
    except:
        return []

# ================= 主程序 =================

# 1. 获取本次任务范围
start_page = get_start_page()
end_page = start_page + PAGES_PER_RUN

print(f"🚀 启动增量爬虫：从第 {start_page + 1} 页爬到第 {end_page} 页...")

all_songs = []

try:
    for page in range(start_page, end_page):
        print(f"\n--- 正在处理第 {page + 1} 页 ---")
        pids = get_playlist_ids_from_page(page)
        
        if len(pids) < 10:
            print("⚠️ 警告：本页获取歌单过少，可能触发反爬，建议暂停。")
        
        for pid in pids:
            songs = get_playlist_songs_api(pid)
            if songs:
                all_songs.extend(songs)
                
            if len(all_songs) % 500 < 50:
                print(f"   -> 本次已新增: {len(all_songs)} 首")

    # 更新进度文件
    save_current_page(end_page)
    print(f"✅ 进度已更新：下次将从第 {end_page + 1} 页开始。")

except KeyboardInterrupt:
    print("\n⚠️ 用户手动停止！正在保存已获取的数据...")
    save_current_page(page) # 保存当前断点

# ================= 数据追加与保存 =================
if all_songs:
    print(f"\n💾 正在保存 {len(all_songs)} 条新数据...")
    
    # 1. 处理歌曲元数据
    df_new_meta = pd.DataFrame(all_songs)
    
    # 检查文件是否存在
    if os.path.exists(FILE_META):
        # 读取旧数据用于去重（避免追加重复歌曲）
        df_old_meta = pd.read_csv(FILE_META)
        # 合并
        df_combined = pd.concat([df_old_meta, df_new_meta])
        # 去重 (保留第一次出现的)
        df_final_meta = df_combined.drop_duplicates(subset=['song_id'])
        # 统计新增
        new_count = len(df_final_meta) - len(df_old_meta)
        print(f"📊 歌曲库更新：原有 {len(df_old_meta)} -> 现有 {len(df_final_meta)} (新增 {new_count} 首)")
    else:
        df_final_meta = df_new_meta.drop_duplicates(subset=['song_id'])
        print(f"📊 新建歌曲库：共 {len(df_final_meta)} 首")
    
    # 写入 CSV (这里我们总是覆盖写全量，保证去重干净)
    os.makedirs('processed', exist_ok=True)
    df_final_meta.to_csv(FILE_META, index=False)

    # 2. 生成并追加交互数据 (Interactions)
    # 我们只为本次新增的歌曲生成交互，避免旧数据重复生成
    # 或者简单粗暴一点：为本次抓取到的所有歌（哪怕重复）都生成一点交互，增加热度
    print("🎲 生成模拟交互数据...")
    user_interactions = []
    # 这里我们只用本次抓取的 song_id
    current_song_ids = df_new_meta['song_id'].tolist()
    
    # 模拟逻辑：假设有 2000 个用户，随机产生交互
    # 为了追加方便，这里直接生成新行
    for _ in range(len(current_song_ids) * 5): # 假设平均每首歌有 5 次新播放
        uid = random.randint(1, 2000)
        sid = random.choice(current_song_ids)
        count = random.randint(1, 50)
        user_interactions.append({'user_id': uid, 'song_id': sid, 'playcount': count})
    
    df_new_inter = pd.DataFrame(user_interactions)
    
    # 追加模式写入 (Header只在文件不存在时写入)
    header_mode = not os.path.exists(FILE_INTERACT)
    df_new_inter.to_csv(FILE_INTERACT, mode='a', header=header_mode, index=False)
    print(f"✅ 交互数据已追加 {len(df_new_inter)} 条记录。")

else:
    print("❌ 本次没有抓取到数据。")
