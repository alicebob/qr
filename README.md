In-process queue with disk based overflow.


When everything is fine elements flow over Qr.q. This is a simple channel
connecting the producer(s) and the consumer(s).
If that channel is full elements are written to the Qr.planb channel.
swapout() will write all elements from Qr.planb to disk. It makes a new file
every `timeout`. At the same time swapin() will deal with completed files.
swapin() will open the oldest file and write the elements to Qr.q.

```
  ---> Enqueue()   ------   .q   ----->    merge() -> .out -> Dequeue() --->
           \                                 ^
         .planb                         .confluence
            \                               /
             \--> swapout()     swapin() --/
                     \             ^
                      \--> fs() --/
```

Gob is used to serialize entries; custom types should be registered using
gob.Register().

Same idea as https://github.com/alicebob/q but cleaner, and this queue doesn't care about keeping things ordered.


# &c.

[![Build Status](https://travis-ci.org/alicebob/qr.svg?branch=master)](https://travis-ci.org/alicebob/qr)
