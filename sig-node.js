const Chord = require('./lib/chord'),
	SocketIO = require('socket.io'),
	co = require('co'),
	http = require('http'),
	url = require('url'),
	wrtc = require('electron-webrtc')({ headless: true }),

	server = http.Server(),
	ioServer = SocketIO(server),
	peerOptions = { wrtc },
	chord = new Chord({ peerOptions }, true),
	sigPeers = { },
	thisUrl = { }

ioServer.on('connection', sock => {
	thisUrl['ws://' + sock.handshake.headers.host + sock.handshake.url] = Date.now()
	co(chord.connectViaWebSocket(sock))
})

server.addListener('request', (req, res) => {
	var reqUrl = req.url.startsWith('/') ? 'http://' + req.headers.host + req.url : req.url,
		parse = url.parse(reqUrl, true)
	if (req.method === 'GET' && parse.path === '/status') {
		var id = chord.id
		res.end(JSON.stringify({ id, sigPeers }))
	}
	else {
		res.end('')
	}
})

chord.on('pub-sig-peer', data => {
	var time = Date.now(),
		urls = data.urls,
		peers = sigPeers

	sigPeers = { }
	Object.keys(peers)
		.filter(id => peers[id].time > time - 5 * 60 * 1000)
		.forEach(id => sigPeers[id] = peers[id])

	if (data.id !== chord.id) {
		sigPeers[data.id] = { urls, time }
	}
})

setInterval(_ => {
	chord.subscribe('sig-peer')
	chord.publish('sig-peer', 'pub-sig-peer', {
		id: this.id,
		urls: Object.keys(thisUrl).sort((a, b) => thisUrl[a] - thisUrl[b]),
	})
	var time = Date.now() - 5 * 60 * 1000,
		availIds = Object.keys(sigPeers),
		peerId = availIds[Math.floor(Math.random() * availIds.length)],
		url = sigPeers[peerId] && sigPeers[peerId].urls[0]
	if (url) {
		co(chord.connectViaWebSocket(sock))
	}
}, 2000)

server.listen(8088, _ => {
	console.log('listening at port ' + server.address().port)
})
