
const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const session = require('express-session');

/**
 * 统一哈希算法：生成 32-bit 安全正整数，完全兼容 Spark 的 abs(hash(col))
 * @param {string} str 
 * @returns {number} 32位以内的安全正整数
 */
function calcSafeHash(str) {
    if (!str) return 0;
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash |= 0; // 转成 32bit 整数
    }
    return Math.abs(hash);
}



const app = express();
const port = 3000;



app.set('view engine', 'ejs');
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json()); // 【新增】允许解析 JSON 请求
app.use(express.static('public'));

app.use(session({
    secret: 'music_secret', resave: false, saveUninitialized: true,
    cookie: { maxAge: 24 * 60 * 60 * 1000 }
}));

const pool = mysql.createPool({
    host: '127.0.0.1',
    user: 'hadoop_master',
    password: 'hadoop',
    database: 'music_rec_sys',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
    supportBigNumbers: true,
    bigNumberStrings: true
});


// 中间件
function requireLogin(req, res, next) {
    if (!req.session.user) return res.redirect('/login');
    res.locals.user = req.session.user; // 让模板也能访问 user
    next();
}
app.use((req, res, next) => { res.locals.user = req.session.user; next(); });

// ================= 基础路由 (登录/注册/首页) =================

app.get('/login', (req, res) => res.render('login', { error: null }));
app.post('/login', (req, res) => {
    const { username, password } = req.body;
    pool.query('SELECT * FROM users WHERE username = ? AND password = ?', [username, password], (err, results) => {
        if (results && results.length > 0) {
            const userRow = results[0];
            req.session.user = {
                uid: userRow.uid,
                username: userRow.username,
                shadow_id: userRow.shadow_id,
                // 数据库读出的bigint转字符串，防止BigInt序列化报错
                stable_uid_hash: String(userRow.stable_uid_hash)
            };
            res.redirect('/');
        } else {
            res.render('login', { error: '账号或密码错误' });
        }
    });
});



app.get('/register', (req, res) => res.render('register', { error: null }));
app.post('/register', (req, res) => {
    const { username, password } = req.body;
    const shadowId = Math.floor(Math.random() * 2000) + 1;
    // =========【改动】计算stable_uid_hash =========
    const stableUidHash = calcSafeHash(username);

    // INSERT增加stable_uid_hash字段
    pool.query('INSERT INTO users (username, password, shadow_id, stable_uid_hash) VALUES (?, ?, ?, ?)', [username, password, shadowId, stableUidHash], (err, res2) => {
        if(err) return res.render('register', {error: '用户名已存在'});
        // session保存stable_uid_hash，后面接口要用
        req.session.user = { 
            uid: res2.insertId, 
            username, 
            shadow_id: shadowId,
            stable_uid_hash: stableUidHash   // 存入session
        };
        res.redirect('/');
    });
});


app.get('/logout', (req, res) => { req.session.destroy(); res.redirect('/login'); });

// ================= 路由 2: 首页 =================
app.get('/', (req, res) => {
    const userId = req.query.userId; // 注意：虽然前端用了Session，但保留这个读取也没事
    const mood = req.query.mood || 'all'; 
    // Session 校验
    if (!req.session.user) return res.redirect('/login');

    // 【修改点】 LIMIT 8 -> LIMIT 9 (为了 3x3 布局)
    let sqlHot = `SELECT * FROM songs WHERE source='netease' ORDER BY RAND() LIMIT 9`; 
    let sqlNew = `SELECT * FROM songs WHERE source='spotify' ORDER BY RAND() LIMIT 8`; // 底部轮播保持 8 或更多都行
    
    if (mood === 'sad') sqlHot = `SELECT * FROM songs ORDER BY song_id ASC LIMIT 9`;
    if (mood === 'happy') sqlHot = `SELECT * FROM songs ORDER BY song_id DESC LIMIT 9`;

    pool.query(sqlHot, (err, hotSongs) => {
        if (err) throw err;
        pool.query(sqlNew, (err, newSongs) => {
            if (err) throw err;
            // 传入 user 对象供导航栏使用
            res.render('home', { user: req.session.user, hotSongs, newSongs, mood });
        });
    });
});


// ================= 核心业务路由 =================

// 歌单详情页 (随机推荐 15 首)
app.get('/playlist/:tag', requireLogin, (req, res) => {
    const tag = req.params.tag;
    // 这里简单实现：随机从库里取 15 首。实际可以根据 Tags 字段筛选 (WHERE tags LIKE %tag%)
    const sql = `SELECT * FROM songs ORDER BY RAND() LIMIT 15`;
    
    pool.query(sql, (err, songs) => {
        res.render('playlist', { tag, songs }); // 需要新建 playlist.ejs
    });
});


// ================= 阶段 3：实时在线重排推荐 =================
app.get('/recommend', requireLogin, (req, res) => {
    const uid = req.session.user.uid;
    const targetStableHash = req.session.user.stable_uid_hash;

    // 1. 获取用户最近 3 次交互的歌曲（即时兴趣触发源）
    const sqlRecent = `
        SELECT song_id FROM user_actions 
        WHERE uid = ? AND action_type IN ('play', 'like') 
        ORDER BY created_at DESC LIMIT 3
    `;

    pool.query(sqlRecent, [uid], (err, recentActions) => {
        const recentSongIds = (recentActions || []).map(r => r.song_id);

        // 2. 查询离线 ALS 生成的基线推荐 (Top 20)
        const sqlOffline = `
            SELECT r.rank_score, s.* 
            FROM recommendations r 
            JOIN songs s ON r.song_id = s.song_id 
            WHERE r.stable_uid_hash = ? 
            ORDER BY r.rank_score DESC LIMIT 20
        `;

        pool.query(sqlOffline, [targetStableHash], (err2, offlineSongs) => {
            let baseList = offlineSongs || [];

            // 兜底逻辑：无推荐时用热门填充
            if (baseList.length === 0) {
                return pool.query(`SELECT * FROM songs ORDER BY RAND() LIMIT 20`, (err3, hot) => {
                    res.render('recommend', { songs: hot || [] });
                });
            }

            // 3. 如果用户近期没有行为，直接返回离线基线
            if (recentSongIds.length === 0) {
                return res.render('recommend', { songs: baseList });
            }

            // 4. 实时召回：利用离线预计算的 related_songs 表进行实时 I2I 召回
            const sqlRealtime = `
                SELECT DISTINCT s.*, rs.similarity_score as rank_score
                FROM related_songs rs
                JOIN songs s ON rs.related_song_id = s.song_id
                WHERE rs.song_id IN (?)
                ORDER BY rs.similarity_score DESC
                LIMIT 6
            `;

            pool.query(sqlRealtime, [recentSongIds], (err4, realtimeSongs) => {
                if (err4 || !realtimeSongs || realtimeSongs.length === 0) {
                    return res.render('recommend', { songs: baseList });
                }

                // 5. 在线混合打散重排 (Real-time Re-ranking)
                // 将实时召回的 4~6 首歌插在推荐列表头部，并去除已听歌曲
                const combined = [];
                const seen = new Set(recentSongIds.map(String));

                // 先放入实时召回的相似歌曲（体现即时响应）
                for (let s of realtimeSongs) {
                    if (!seen.has(String(s.song_id))) {
                        seen.add(String(s.song_id));
                        s.tag = "实时猜你喜欢"; // 页面标识
                        combined.push(s);
                    }
                }

                // 接着填充离线基线歌曲（保证长周期全局偏好）
                for (let s of baseList) {
                    if (!seen.has(String(s.song_id)) && combined.length < 20) {
                        seen.add(String(s.song_id));
                        combined.push(s);
                    }
                }

                res.render('recommend', { songs: combined });
            });
        });
    });
});

// 播放页 (这里不再自动插入 user_actions，改为前端触发)

// ================= 播放页 =================
app.get('/player/:songId', requireLogin, (req, res) => {
    const songId = req.params.songId;
    const uid = req.session.user.uid;
    
    console.log("🎵 访问播放页:", songId);

    pool.query(`SELECT * FROM songs WHERE song_id = ?`, [songId], (err, result) => {
        if (err || result.length === 0) {
            console.error("❌ 歌曲不存在:", songId);
            return res.status(404).send("歌曲不存在");
        }
        
        const song = result[0];
        
        // 查询相似歌曲
        const sqlSim = `
            SELECT DISTINCT s.*, rs.similarity_score 
            FROM related_songs rs 
            JOIN songs s ON rs.related_song_id = s.song_id 
            WHERE rs.song_id = ? 
            ORDER BY rs.similarity_score DESC 
            LIMIT 10
        `;
        
        pool.query(sqlSim, [songId], (err, simSongs) => {
            // ✅ 兜底：查询失败或无结果时使用随机歌曲
            if (err || !simSongs || simSongs.length === 0) {
                console.warn("⚠️ 相似歌曲为空，使用热门兜底");
                pool.query(
                    `SELECT *, 0.5 as similarity_score FROM songs WHERE song_id != ? ORDER BY RAND() LIMIT 5`,
                    [songId],
                    (err2, fallback) => {
                        renderPage(song, fallback || []);
                    }
                );
                return;
            }
            renderPage(song, simSongs);
        });
        
        function renderPage(song, simSongs) {
            const sqlIsLiked = `SELECT 1 FROM ephemeral_likes WHERE uid = ? AND song_id = ?`;
            const sqlComments = `
                SELECT c.*, u.username 
                FROM comments c 
                JOIN users u ON c.uid = u.uid 
                WHERE c.song_id = ? 
                ORDER BY c.created_at DESC
            `;
            
            pool.query(sqlIsLiked, [uid, songId], (err, likeResult) => {
                const isLiked = likeResult && likeResult.length > 0;
                
                pool.query(sqlComments, [songId], (err, comments) => {
                    if (err) comments = [];
                    
                    // ✅ 确保所有变量都传递
                    res.render('player', {
                        song,
                        simSongs: simSongs || [],
                        isLiked,
                        comments: comments || [],
                        user: req.session.user  // ✅ 关键：传递 user
                    });
                });
            });
        }
    });
});


// 个人中心 (聚合查询) — 当前用户
app.get('/profile', requireLogin, (req, res) => {
    const uid = req.session.user.uid;

    // 首先读取用户信息（包括 public_playlists）
    pool.query('SELECT * FROM users WHERE uid = ?', [uid], (err, users) => {
        if (err) throw err;
        const pageUser = users[0] || req.session.user;

        // 1. 最近播放 (History)
        const sqlHistory = `SELECT DISTINCT s.song_id, s.title, s.artist, s.image_url, ua.created_at FROM user_actions ua JOIN songs s ON ua.song_id = s.song_id WHERE ua.uid = ? AND ua.action_type = 'play' ORDER BY ua.created_at DESC LIMIT 30`;
        
        // 2. 我喜欢的 (Likes)
        const sqlLikes = `SELECT s.song_id, s.title, s.artist, s.image_url, el.expire_at FROM ephemeral_likes el JOIN songs s ON el.song_id = s.song_id WHERE el.uid = ? ORDER BY el.expire_at DESC`;

        // 3. 我的评论 (Comments)
        const sqlComments = `SELECT c.content, c.created_at, s.title, s.song_id FROM comments c JOIN songs s ON c.song_id = s.song_id WHERE c.uid = ? ORDER BY c.created_at DESC`;

        pool.query(sqlHistory, [uid], (err, history) => {
            pool.query(sqlLikes, [uid], (err, likes) => {
                pool.query(sqlComments, [uid], (err, myComments) => {
                    res.render('profile', { history, likes, myComments, viewUser: pageUser, isOwner: true });
                });
            });
        });
    });
});

// 个人页：按用户名查看（方案一实现）
app.get('/profile/:username', requireLogin, (req, res) => {
    const username = req.params.username;
    pool.query('SELECT * FROM users WHERE username = ?', [username], (err, rows) => {
        if (err) throw err;
        if (!rows || rows.length === 0) return res.status(404).send('用户不存在');
        const viewUser = rows[0];
        const isOwner = req.session.user && req.session.user.uid === viewUser.uid;

        // 如果不是自己且对方未公开歌单，则只显示有限信息
        if (!isOwner && !viewUser.public_playlists) {
            return res.render('profile', { history: [], likes: [], myComments: [], viewUser, isOwner: false });
        }

        const sqlHistory = `SELECT DISTINCT s.song_id, s.title, s.artist, s.image_url, ua.created_at FROM user_actions ua JOIN songs s ON ua.song_id = s.song_id WHERE ua.uid = ? AND ua.action_type = 'play' ORDER BY ua.created_at DESC LIMIT 30`;
        const sqlLikes = `SELECT s.song_id, s.title, s.artist, s.image_url, el.expire_at FROM ephemeral_likes el JOIN songs s ON el.song_id = s.song_id WHERE el.uid = ? ORDER BY el.expire_at DESC`;
        const sqlComments = `SELECT c.content, c.created_at, s.title, s.song_id FROM comments c JOIN songs s ON c.song_id = s.song_id WHERE c.uid = ? ORDER BY c.created_at DESC`;

        pool.query(sqlHistory, [viewUser.uid], (err, history) => {
            pool.query(sqlLikes, [viewUser.uid], (err, likes) => {
                pool.query(sqlComments, [viewUser.uid], (err, myComments) => {
                    res.render('profile', { history, likes, myComments, viewUser, isOwner });
                });
            });
        });
    });
});


// ================= API 接口 (供前端 JS 调用) =================
app.post('/api/log_action', requireLogin, (req, res) => {
    let { songId, actionType, content } = req.body; 
    const uid = req.session.user.uid;
    const stableUidHash = req.session.user.stable_uid_hash;

    console.log(`[用户行为] uid=${uid}, songId=${songId}, action=${actionType}`);

    if (!songId) {
        return res.status(400).json({ success: false, msg: "缺少 songId" });
    }

    const weightMap = { 'play': 1, 'like': 3, 'comment': 5, 'unlike': 0 };
    const weight = weightMap[actionType] !== undefined ? weightMap[actionType] : 1;

    // 1. 写入 user_actions 表
    const insertActionSql = `
        INSERT INTO user_actions (uid, stable_uid_hash, song_id, action_type, weight) 
        VALUES (?, ?, ?, ?, ?)
    `;

    pool.query(insertActionSql, [uid, stableUidHash, songId, actionType, weight], (err) => {
        if (err) {
            console.error("❌ 写入 user_actions 失败:", err.sqlMessage || err);
            return res.status(500).json({ success: false, msg: "写入 user_actions 失败: " + (err.sqlMessage || "") });
        }

        // 2. 分流处理业务
        if (actionType === 'like') {
            const expireDate = new Date();
            expireDate.setDate(expireDate.getDate() + 30);
            pool.query('INSERT IGNORE INTO ephemeral_likes (uid, song_id, expire_at) VALUES (?, ?, ?)', 
                [uid, songId, expireDate], (err2) => {
                    if (err2) {
                        console.error("❌ 写入 ephemeral_likes 失败:", err2.sqlMessage || err2);
                        return res.status(500).json({ success: false, msg: err2.sqlMessage });
                    }
                    console.log(" 点赞成功");
                    return res.json({ success: true, msg: 'Liked' });
                }
            );
        } else if (actionType === 'unlike') {
            pool.query('DELETE FROM ephemeral_likes WHERE uid = ? AND song_id = ?', 
                [uid, songId], (err2) => {
                    if (err2) {
                        console.error("❌ 删除 ephemeral_likes 失败:", err2.sqlMessage || err2);
                        return res.status(500).json({ success: false, msg: err2.sqlMessage });
                    }
                    console.log(" 取消点赞成功");
                    return res.json({ success: true, msg: 'Unliked' });
                }
            );
        } else if (actionType === 'comment') {
            pool.query('INSERT INTO comments (uid, song_id, content) VALUES (?, ?, ?)', 
                [uid, songId, content], (err2) => {
                    if (err2) {
                        console.error("❌ 写入 comments 失败:", err2.sqlMessage || err2);
                        return res.status(500).json({ success: false, msg: err2.sqlMessage });
                    }
                    console.log(" 评论发布成功");
                    return res.json({ success: true, msg: 'Commented' });
                }
            );
        } else {
            return res.json({ success: true, msg: 'Play recorded' });
        }
    });
});

// 设置是否公开歌单（用户控制）
app.post('/api/set_public_playlists', requireLogin, (req, res) => {
    const value = req.body.value ? 1 : 0;
    const uid = req.session.user.uid;
    pool.query('UPDATE users SET public_playlists = ? WHERE uid = ?', [value, uid], (err, result) => {
        if (err) return res.json({ success: false });
        // 同步到 session
        req.session.user.public_playlists = value;
        res.json({ success: true, value });
    });
});

// 【替换】封面搜索接口 (改用 iTunes API，无需翻墙，且 VM 通常能访问)
app.get('/api/cover_search', async (req, res) => {
    const { title, artist } = req.query;
    if (!title) return res.status(400).json({ error: 'Missing parameters' });

    try {
        const term = encodeURIComponent(`${title} ${artist}`);
        // 增加 entity=song 提高准确率
        const url = `https://itunes.apple.com/search?term=${term}&media=music&entity=song&limit=1`;
        
        const response = await fetch(url);
        
        // 检查状态码
        if (!response.ok) throw new Error(`HTTP Status ${response.status}`);
        
        // 先取文本，防止空响应炸毁 JSON.parse
        const text = await response.text();
        if (!text) return res.status(404).json({ error: 'Empty response' });

        const data = JSON.parse(text);

        if (data.resultCount > 0) {
            const artworkUrl = data.results[0].artworkUrl100.replace('100x100bb', '600x600bb');
            res.json({ url: artworkUrl });
        } else {
            res.status(404).json({ error: 'Not found' });
        }
    } catch (e) {
        // 仅在非网络错误时打印，保持控制台干净
        if (e.code !== 'ECONNRESET' && e.code !== 'ETIMEDOUT') {
            // console.error("iTunes API Warning:", e.message); // 注释掉，不想看报错就彻底静音
        }
        res.status(500).json({ error: 'Network error' });
    }
});

// 授权删除接口：仅允许本人删除自己的临时喜欢或自己的评论
app.post('/api/delete_item', requireLogin, (req, res) => {
    const type = req.body.type;
    const uid = req.session.user.uid;

    if (type === 'comment') {
        const id = req.body.id;
        if (!id) return res.json({ success: false });
        pool.query('SELECT uid FROM comments WHERE id = ?', [id], (err, rows) => {
            if (err) return res.json({ success: false });
            if (!rows || rows.length === 0) return res.json({ success: false });
            if (rows[0].uid !== uid) return res.json({ success: false, msg: 'Unauthorized' });
            pool.query('DELETE FROM comments WHERE id = ?', [id], (err2) => {
                if (err2) return res.json({ success: false });
                return res.json({ success: true });
            });
        });
    } else if (type === 'like') {
        const songId = req.body.songId;
        if (!songId) return res.json({ success: false });
        pool.query('DELETE FROM ephemeral_likes WHERE uid = ? AND song_id = ?', [uid, songId], (err) => {
            if (err) return res.json({ success: false });
            return res.json({ success: true });
        });
    } else {
        return res.json({ success: false });
    }
});



app.listen(port, () => console.log(`Server running at http://localhost:${port}/login`));
