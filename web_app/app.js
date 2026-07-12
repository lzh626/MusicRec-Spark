const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const session = require('express-session');
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
    host: 'localhost', user: 'hadoop_master', password: 'hadoop',
    database: 'music_rec_sys', waitForConnections: true, connectionLimit: 10, queueLimit: 0
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
            req.session.user = results[0];
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
    pool.query('INSERT INTO users (username, password, shadow_id) VALUES (?, ?, ?)', [username, password, shadowId], (err, res2) => {
        if(err) return res.render('register', {error: '用户名已存在'});
        req.session.user = { uid: res2.insertId, username, shadow_id: shadowId };
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



// 每日推荐
app.get('/recommend', requireLogin, (req, res) => {
    const refresh = req.query.refresh;
    const targetId = req.session.user.shadow_id;
    let orderBy = refresh === 'true' ? 'RAND()' : 'r.rank_score DESC';
    const sql = `SELECT r.rank_score, s.* FROM recommendations r JOIN songs s ON r.song_id = s.song_id WHERE r.user_id = ? ORDER BY ${orderBy} LIMIT 20`;
    pool.query(sql, [targetId], (err, results) => res.render('recommend', { songs: results }));
});

// 播放页 (这里不再自动插入 user_actions，改为前端触发)
app.get('/player/:songId', requireLogin, (req, res) => {
    const songId = req.params.songId;
    const uid = req.session.user.uid;

    pool.query(`SELECT * FROM songs WHERE song_id = ?`, [songId], (err, result) => {
        if (result.length === 0) return res.send("404 Not Found");
        const song = result[0];
        
        // 并行查询：相似歌曲 + 当前用户是否喜欢过 + 当前用户的评论
        const sqlSim = `SELECT s.*, rs.similarity_score FROM related_songs rs JOIN songs s ON rs.related_song_id = s.song_id WHERE rs.song_id = ? ORDER BY rs.similarity_score DESC LIMIT 10`;
        const sqlIsLiked = `SELECT 1 FROM ephemeral_likes WHERE uid = ? AND song_id = ?`;
        const sqlComments = `SELECT c.*, u.username FROM comments c JOIN users u ON c.uid = u.uid WHERE c.song_id = ? ORDER BY c.created_at DESC`;

        pool.query(sqlSim, [songId], (err, simSongs) => {
            pool.query(sqlIsLiked, [uid, songId], (err, likeResult) => {
                const isLiked = likeResult.length > 0;
                pool.query(sqlComments, [songId], (err, comments) => {
                    res.render('player', { song, simSongs, isLiked, comments });
                });
            });
        });
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

// 通用行为记录接口
app.post('/api/log_action', requireLogin, (req, res) => {
    const { songId, actionType, content } = req.body; // actionType: 'play', 'like', 'comment'
    const uid = req.session.user.uid;
    const weight = actionType === 'play' ? 1 : (actionType === 'like' ? 3 : 5);

    // 1. 写入 user_actions (用于Spark训练)
    pool.query('INSERT INTO user_actions (uid, song_id, action_type, weight) VALUES (?, ?, ?, ?)', 
        [uid, songId, actionType, weight]);

    // 2. 额外逻辑
    if (actionType === 'like') {
        // 喜欢：存入 ephemeral_likes，过期时间设为30天后
        const expireDate = new Date();
        expireDate.setDate(expireDate.getDate() + 30);
        pool.query('INSERT IGNORE INTO ephemeral_likes (uid, song_id, expire_at) VALUES (?, ?, ?)', [uid, songId, expireDate]);
        res.json({ success: true, msg: 'Liked' });
    } else if (actionType === 'unlike') {
        // 取消喜欢
        pool.query('DELETE FROM ephemeral_likes WHERE uid = ? AND song_id = ?', [uid, songId]);
        res.json({ success: true, msg: 'Unliked' });
    } else if (actionType === 'comment') {
        // 评论
        pool.query('INSERT INTO comments (uid, song_id, content) VALUES (?, ?, ?)', [uid, songId, content]);
        res.json({ success: true, msg: 'Commented' });
    } else {
        // 播放
        res.json({ success: true, msg: 'Played logged' });
    }
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
