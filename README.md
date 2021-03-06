# Bagger

This is a Java BagIt library that takes an opinionated view of how to create bags
based primarily based on experience from dealings with snapshots pull from Duracloud.
There is still a bit of variance as everything gets sorted out and the regular data
flow becomes well defined.

# JavaDoc

Hosted online one day

# Usage

## Creating Bags

### Loading a manifest

In order to create a bag, a set of files needs to be associated. Typically this is done
through a PayloadManifest which tracks the Digest type, Digest, and PayloadFiles of a
collection.

```java
Path base = Paths.get("/path/to/files/");
PayloadManifest manifest = PayloadManifest.loadFromStream(
    Files.newInputStream(base.resolve("manifest-sha256.txt"), 
    base);
```

_Note: A typical layout emulates the bagit structure_
```
[/path/to/files/] $ tree
.
├── data
│   ├── ...
├── extra-tag.txt
├── manifest-md5.txt
└── manifest-sha256.txt
```

### Partitioning

The first step of creating a bag or set of bags is to create something which will partition
them for you. The Bagger class will do this for you, allowing you to specify a naming scheme
for your bags, a maximum size, and a base BagInfo which will be shared throughout all bags.

```java
// Create a Bagger object which can partition the PayloadManifest
Bagger baggins = new Bagger()
    .withBagInfo(new BagInfo())
    .withMaxSize(300, Unit.MEGABYTE)
    .withPayloadManifest(payloadManifest)
    .withNamingSchema(new UUIDNamingSchema());

// Partition our bags
BaggingResult result = baggings.partition(); 
List<Bag> bags = result.getBags();
```

After partitioning, the result can be read to see if everything was successful. For example,
if a file is too large it will be rejected and not added to a bag.

### Writing Bags

Bags are written through the BagWriter interface, which writes synchronously by default and
provides a method which allows an executor to be passed in for asynchronous writing. When writing
async a CompletableFuture is returned when writing so you can process the result of writing at a 
later point in time. By default a SingleThreadExecutor is used if none is specified.

```java
BagWriter writer = new SimpleBagWriter()
    .validate(true)
    .withPackager(new TarPackager(outputDirectory));

List<WriteResult> written = writer.write(bags);
```

### Post Write 

After the bags are written, you can view the results to see if they were successful. A simple example
to view the status of our writing.

```java
// Check all of the results to make sure they are successful
boolean writeSuccess = written.stream().allMatch(WriteResult::isSuccess);
```

A Comparable way to do this if the asynchronous method was chosen instead
```java
List<CompletableFuture<WriteResult>> written = writer.write(bags);

// Join the threads 
List<WriteResult> collect = written.stream()
    .map(CompletableFuture::join)
    .collect(Collectors.toList());

// We check after all threads have joined as a precaution in this example
// Otherwise we could end up short circuiting if a single bag fails to write
boolean writeSuccess = collect.stream().allMatch(WriteResult::isSuccess);
```

_Note: It may also be good to convert from a `List<CompleteableFuture<WriteResult>>`
to `CompletableFuture<List<WriteResult>>`, but this is all new so not much has been done yet_

## Deprecated Classes

Version 1.0 had partitioning and writing of bags handled through the Writer, SimpleWriter, and MultipartWriter classes.
These have been deprecated and will be removed by the 1.2.0 release.
