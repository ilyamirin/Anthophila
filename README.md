Anthophila
==========

Fast and simple key-value warehouse.

I developed this tool for keeping 64kb parts of files. It may be useful if you are cloud file storage, you consist of a cloudy file system tree and a file store. Keeping files as chunks let you do some tricks as a deduplication or versioning, you know:) And, of course, you may keep all possible information as byte array, but this tool will not be effective in this case.

Anthophila is simple. There is no megaframeworks, just a few classes.

Anthophila is cheap. You do not need many developers to maintain.

Anthophila is fast. I believe in async sockets, binary protocols, in-memory indexes, simple syncronisation and static typing.

Anthophila uses little disk space. If you process previously defined blocks of bytes, please do not ask me for compaction, repair of another bullshit maintenance.

Anthophila is secure. It uses Salsa20 with a simple and secure key management for encryption of data. 
