const Chord = require('./lib/chord'),
	SocketIO = require('socket.io'),
	http = require('http'),
	wrtc = require('wrtc'),

	server = http.Server(),
	ioServer = SocketIO(server),
	peerOptions = { wrtc }

new Chord({ peerOptions }).start({ ioServer })

server.listen(8088, _ => console.log('listening at port ' + server.address().port))