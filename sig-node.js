const Chord = require('./lib/chord'),
	SocketIO = require('socket.io'),
	co = require('co'),
	http = require('http'),
	wrtc = require('wrtc'),

	server = http.Server(),
	ioServer = SocketIO(server),
	peerOptions = { wrtc },
	chord = new Chord({ peerOptions }, true)

ioServer.on('connection', sock => {
	sock.once('join', _ => {
		sock.emit('joined', true)
		co(chord.connectViaWebSocket(sock))
	})
})

server.listen(8088, _ => {
	console.log('listening at port ' + server.address().port)
})
