# Purely *On-Disk* Linear Hashing


This is a reference implementation of Litwin's [[1]] Linear hashing algorithm; this was a part of my VFS 
File System project that I did a while back; after some requests I decided to polish the sources and make it
available online (in three separate projects: [B+Tree][2], Linear Hashing and Dynamic Hashing). This package meets the following requirements:

 * Purely disk based
 * Uses strict paging sizes
 * Unique-key storage **only**.
 * Depending on workload is quite fast.
 
# Ease of use features

This project uses maven for easy import to any supporting IDE or build environments, so you should not have any
particular hurdles if you wish to play around with the code.

# Example usage

In the following section we will cover (with examples) how one might go about using this library.

## Instantiation

You could use this in two basic ways; the first is simply to use the default parameters and only 
provide the output file name as is shown below:

```java
LinearHash lin_hash = new LinearHash("filename");
```

The other way is to use the custom constructor, which enables you to tweak various parameters to your liking, an
example of that use case is shown below:

```java
LinearHash slh = new LinearHash("fname",         // filename
                                keysPerBlock,   // keys per each block
					            initial_pool,   // initial (visible) block pool
					            ilb_2,          // insert balance factor
					            dlb_2,          // delete balance factor
					            true,           // override flag
					            epoch_thresh);  // epoch threshold (for tracking)
```

## Insertions

Inserting a key `s` in our store is pretty straightforward; this can
be done as follows:

```java
slh.insertKey(s)
```

Again we can do some error checking to see if the insertion was
successful as is shown below:

```java
if(!slh.insertKey(s)) {
    // handle error
} else {
    // handle success
}
```

## Fetching

Fetching a key `s` from our store is really simple and this 
is done as follows:

```java
slh.fetchKey(s)
```

You can also so dome error checking, as a null value is returned in
case of search failure as such:

```java
int ret;
if((ret = slh.fetchKey(s)) == null) {
    // handle error
} else {
    // do stuff with ret.
}
```

## Deletes

We can delete in a very similar way, assuming we have to delete a key `s` 
then the code to do so would be:

```java
slh.deleteKey(s)
```

You can also do some error checking, like so:

```java
if(!slh.deleteKey(s)) {
    // handle error
} else {
    // handle success
}
```

# License

This work is licensed under the terms and conditions of GPLv2.


# Final notes

There is a small annoyance which will be fixed in a coming patch; when you 
try to reuse a file that has a different configuration than the one supplied 
the behaviour is **unpredicted**.  I have to implement a header to store the 
existing configuration hassle-free.


[1]: http://www.cs.cmu.edu/~christos/courses/826-resources/PAPERS+BOOK/linear-hashing.PDF
[2]: https://github.com/andylamp/BPlusTree 