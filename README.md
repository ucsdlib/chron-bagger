# Bagger

This is a Java BagIt library that takes an opinionated view of how to create bags
based, primarily based on experience from dealings with Duracloud and the like.
If you don't like something, request a change or make a pull request to change something,
there's still a lot to be updated and validated.

# JavaDoc

Hosted online one day

# Usage

## Creating Bags

### Loading a manifest

In order to create a bag, a set of files needs to be associated. Typically this is done
through a PayloadManifest which tracks the Digest type, Digest, and PayloadFiles of a
collection.

```java
Path base = Paths.get("/path/to/manifest/");
PayloadManifest manifest = PayloadManifest.loadFromStream(
    Files.newInputStream(base.resolve("manifest-sha256.txt"), 
    base);
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

Bags are written through the BagWriter interface, which provides a way to supply an Executor
for asynchronous writing. A CompletableFuture is returned when writing so you can process the
result of writing at a later point in time. By default a SingleThreadExecutor is used if none
is specified.

```java
BagWriter writer = new SimpleBagWriter()
    .validate(true)
    .withPackager(new TarPackager(outputDirectory));

List<CompletableFuture<WriteResult>> written = writer.write(bags);
```

### Post Write 

After the bags are written, you can view the results to see if they were successful. A simple example
to view the status of our writing.

```java
// Join the threads 
List<WriteResult> collect = written.stream()
    .map(CompletableFuture::join)
    .collect(Collectors.toList());

// Check all of them to make sure they are successful. Done after
// as a precaution that all threads have joined.
boolean writeSuccess = collect.stream().allMatch(WriteResult::isSuccess);
```
