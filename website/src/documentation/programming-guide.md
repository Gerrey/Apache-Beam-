---
layout: section
title: "Beam Programming Guide"
section_menu: section-menu/documentation.html
permalink: /documentation/programming-guide/
redirect_from:
  - /learn/programming-guide/
  - /docs/learn/programming-guide/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Beam 编程指南

**Beam编程指南**适用于希望使用Beam SDK创建数据处理管道的Beam用户。 它为使用Beam SDK类构建和测试您的管道提供了指导。 它不是一个详细且全面的参考，而是一个与语言无关的高级指南，以编程方式构建您的Beam管道。 在编写本指南时，包含多种语言的代码示例，用来帮助说明如何在您的管道中实现Beam概念。

<nav class="language-switcher">
  <strong>适应于:</strong>
  <ul>
    <li data-type="language-java" class="active">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>


## 1. 概述{#overview}

为了使用Beam，您首先需要使用其中一种Beam SDK中的类来创建一个驱动程序。 您的驱动程序*定义*了您的管道，包括所有输入，转换和输出; 
它还为您的管道设置执行选项（通常使用命令行选项传递）。 其中包括Pipeline Runner，它反过来确定您的管道将运行在哪个后端。


Beam SDK提供了许多抽象概念，简化了大规模分布式数据处理的机制。 相同的Beam抽象适用于批处理和流数据源。 当您创建Beam管道时，您可以根据这些抽象来考虑数据处理任务。 它们包括：

* `Pipeline`: 一个 `Pipeline`，从头到尾封装了您的整个数据处理任务。这包括读取输入数据，转换数据和写入输出数据。所有Beam驱动程序都必须创建一个 `Pipeline`。当您创建 `Pipeline`时，还必须指定执行选项，这些选项告诉 `Pipeline`，在何处以及如何运行。

* `PCollection`: 一个 `PCollection` 代表着您Beam管道操作的分布式数据集。数据集可以是*有界*的，这意味着它来自像文件这样的固定源， 或者也可以是*无界*的，这意味着它来自通过订阅或其他机制不断更新的源。 您的管道通常通过从外部数据源读取数据来创建初始的 `PCollection`，但您也可以从驱动程序中的内存数据创建一个 `PCollection`。从那里开始， `PCollection` 将作为您管道中每个步骤的输入和输出。

* `PTransform`: 一个 `PTransform` 代表着您管道中的数据处理操作或步骤。 每个 `PTransform` 都将一个或多个 `PCollection` 对象作为输入，执行您在该
  `PCollection` 的元素上提供的处理函数，并生成零个或多个输出 `PCollection` 对象。

* I/O 转换: Beam附带了许多“IO”——`PTransform` 库，它可以读或写数据到各种外部存储系统。

一个典型的Beam驱动程序的工作原理如下：:

* **创建** 一个 `Pipeline` 对象并设置管道执行选项，包括Pipeline Runner。
* 为管道数据创建初始 `PCollection`， 使用IO从外部存储系统读取数据，或使用 `Create` 转换从内存数据中构建的一个 `PCollection`。
* **应用** 将 `PTransforms` 应用于每个 `PCollection`。变换可以更改，过滤，分组，分析或以其他方式处理 `PCollection` 中的元素。 转换在*不修改输入集合*的情况下，创建一个新的输出 `PCollection`。 一个典型的管道依次对每个新的输出 `PCollection` 应用后续转换，直到处理完成。 请注意，管道不一定是一个接一个应用的单个直线转换：将 `PCollection` 视为变量，将 `PTransform` 视为应用于这些变量的函数：管道的形状可以是任意复杂的处理图。
* 使用IO，将最终的转换后的 `PCollection` 写入外部源。
* **运行** 管道， 使用指派的 Pipeline Runner。

当您运行您的Beam驱动程序时，您指派的Pipeline Runner会根据您创建的 `PCollection`对象和应用的转换为您的管道构建一个**工作流图** 。然后使用适当的分布式处理后端执行该图，从而成为该后端的异步“作业”(或等效的)。

## 2. 创建一个管道 {#creating-a-pipeline}

`Pipeline` 抽象封装了您数据处理任务中的所有数据和步骤。  您的Beam驱动程序通常首先构建一个
[Pipeline](https://beam.apache.org/releases/javadoc/2.12.0/index.html?org/apache/beam/sdk/Pipeline.html) [Pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/pipeline.py) 对象, ，然后使用该对象作为基础， 用于将管道的数据集创建为 `PCollection`，并将它的操作创建为 `Transform`。

为了使用Beam，您的驱动程序必须首先创建Beam SDK类 `Pipeline` 的实例 (通常在 `main()` 函数中). 当您创建您的 `Pipeline` 时，您还需要设置一些**配置选项**。 您可以以编程方式设置管道的配置选项，, 但通常更容易的做法是提前设置选项（或从命令行读取它们），并在创建对象时将它们传递给 `Pipeline` 对象。

```java
// 首先定义管道的选项。
PipelineOptions options = PipelineOptionsFactory.create();

// 然后创建管道。
Pipeline p = Pipeline.create(options);
```
```py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())
```
```go
// 为了开始创建用于执行的管道，需要Pipeline对象和Scope对象。
p, s := beam.NewPipelineWithRoot()
```

### 2.1. 配置管道选项 {#configuring-pipeline-options}

使用管道选项来配置您管道的不同方面，例如，将执行管道的管道运行程序以及所选运行程序所需的任何特定于运行程序的配置。 您的管道选项可能包含诸如项目ID或存储文件的位置之类的信息。

当在您选择的运行器上运行管道时，您的代码将可以使用PipelineOptions的副本。例如，如果将一个PipelineOptions参数添加到DoFn的 `@ProcessElement` 方法，它将由系统填充。

#### 2.1.1. 从命令行参数设置PipelineOptions {#pipeline-options-cli}

虽然您可以通过创建一个 `PipelineOptions` 对象并直接设置字段来配置管道，但Beam SDK包含一个命令行解析器，您可以通过它使用命令行参数在 `PipelineOptions` 中设置字段。

为了从命令行中读取选项，请构造您的 `PipelineOptions` 对象，如以下示例代码所示：

```java
PipelineOptions options =
    PipelineOptionsFactory.fromArgs(args).withValidation().create();
```
```py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

p = beam.Pipeline(options=PipelineOptions())
```
```go
//如果使用beamx或Go标志，则必须首先解析标志。
flag.Parse()
```



这解释了以下格式的命令行参数：

```
--<option>=<value>
```

> **注意:** 追加方法 `.withValidation` 将检查所需的命令行参数并验证参数值。

以这种方式构建您的 `PipelineOptions`，允许您将任何选项指定为命令行参数。

> **注意:** [WordCount管道示例](https://beam.apache.org/get-started/wordcount-example/) 演示了如何使用命令行选项在运行时设置管道选项。

#### 2.1.2. 创建自定义选项 {#creating-custom-options}

除标准 `PipelineOptions` 外，您还可以添加您自己的自定义选项。为了添加您自己的选项，请为每个选项定义带有getter和setter方法的接口，如下面的示例所示，用于添加 `input` 和 `output` 自定义选项:

```java
public interface MyOptions extends PipelineOptions {
    String getInput();
    void setInput(String input);
    
    String getOutput();
    void setOutput(String output);
}
```
```py
class MyOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input')
    parser.add_argument('--output')
```
```go
var (
  input = flag.String("input", "", "")
  output = flag.String("output", "", "")
)
```

您还可以指定一个描述，该描述在用户将 `--help` 作为命令行参数传递时显示，并显示默认值。

您可以使用注解设置描述和默认值，如下所示：

```java
public interface MyOptions extends PipelineOptions {
    @Description("Input for the pipeline")
    @Default.String("gs://my-bucket/input")
    String getInput();
    void setInput(String input);

    @Description("Output for the pipeline")
    @Default.String("gs://my-bucket/input")
    String getOutput();
    void setOutput(String output);
}
```
```py
class MyOptions(PipelineOptions):

  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('--input',
                        help='Input for the pipeline',
                        default='gs://my-bucket/input')
    parser.add_argument('--output',
                        help='Output for the pipeline',
                        default='gs://my-bucket/output')
```
```go
var (
  input = flag.String("input", "gs://my-bucket/input", "Input for the pipeline")
  output = flag.String("output", "gs://my-bucket/output", "Output for the pipeline")
)
```
我们建议您使用 `PipelineOptionsFactory` 注册您的接口，然后在创建 `PipelineOptions` 对象时传递该接口。 当您使用 `PipelineOptionsFactory` 注册您的接口时，`--help` 可以找到您的自定义选项接口并将它添加到 `--help` 命令的输出中。 `PipelineOptionsFactory` 还将验证您的自定义选项是否与所有其他已注册的选项兼容。


下面的示例代码，显示了如何使用 `PipelineOptionsFactory` 来注册您的自定义选项接口：

```java
PipelineOptionsFactory.register(MyOptions.class);
MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                .withValidation()
                                                .as(MyOptions.class);
```

现在，您的管道可以接受 `--input=value` 和 `--output=value` 作为命令行参数。

## 3. PCollections {#pcollections}

[PCollection](https://beam.apache.org/releases/javadoc/2.12.0/index.html?org/apache/beam/sdk/values/PCollection.html)
`PCollection` 抽象表示一个潜在分布的，多元素数据集。 您可以将 `PCollection` 视为“管道”数据; Beam的变换使用 `PCollection` 对象作为输入和输出。  因此，如果你要处理您管道中的数据，就必须采用 `PCollection` 的形式。

在您创建您的 `Pipeline` 之后，您首先需要以某种形式创建至少一个  `PCollection` 。 您创建的  `PCollection` 将用作管道中第一个操作的输入。

### 3.1. 创建一个PCollection {#creating-a-pcollection}

您可以通过使用Beam的[Source API](#pipeline-io)从外部源读取数据来创建一个 `PCollection` ，也可以在您的驱动程序中，创建一个存储在内存集合类中的数据的 `PCollection` 。前者通常是如何获取数据的生产管道; Beam的Source API包含了适配器，可以帮助您从外部源读取大型基于云的文件，数据库或订阅服务。后者主要是用于测试和调试的目的。

#### 3.1.1. 从外部源读取 {#reading-external-source}

要从外部源读取数据，请使用Beam提供的一个[I/O适配器](#pipeline-io)。 每个适配器的确切用法各不相同，但它们都从一些外部数据源读取数据，并返回一个表示该源中数据记录的 `PCollection` 元素。

每个数据源适配器都有一个 `Read` 变换;要读取，您必须将该变换应用于 `Pipeline` 对象本身。例如，`TextIO.Read`，`io.TextFileSource` 从外部文本文件读取并返回元素类型为 `String` 的 `PCollection`，每个 `String` 表示文本文件中的一行。 下面是将`TextIO.Read`，`io.TextFileSource` 应用于管道以创建 `PCollection` 的方法：

```java
public static void main(String[] args) {
    // 创建管道.
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // 通过应用“Read”变换创建PCollection“线”。
    PCollection<String> lines = p.apply(
      "ReadMyFile", TextIO.read().from("gs://some/inputData.txt"));
}
```
```py
lines = p | 'ReadMyFile' >> beam.io.ReadFromText('gs://some/inputData.txt')
```
```go
lines := textio.Read(s, "gs://some/inputData.txt")
```

请参阅有关[I/O](#pipeline-io)的部分，以了解有关如何从Beam SDK支持的各种数据源进行读取的更多信息。

#### 3.1.2. 从内存数据中创建PCollection {#creating-pcollection-in-memory}


要从内存中的Java `Collection` 中创建一个 `PCollection`，请使用Beam提供的 `Create` 转换。与数据适配器的 `Read` 非常相似，您可以直接将 `Create` 应用于 `Pipeline` 对象本身。

作为参数，`Create` 接受Java `Collection` 和一个 `Coder` 对象。`Coder` 指定了如何[编码](#element-type) `Collection` 中的元素。

要从内存 `list` 创建一个 `PCollection`，请使用Beam提供的 `Create` 变换。 将此变换直接应用于 `Pipeline` 对象本身。

下面的示例代码显示了如何从内存`List` `list` 创建一个 `PCollection`：

```java
public static void main(String[] args) {
    //创建一个Java集合, 在这个例子中是一个字符串列表.
    final List<String> LINES = Arrays.asList(
      "To be, or not to be: that is the question: ",
      "Whether 'tis nobler in the mind to suffer ",
      "The slings and arrows of outrageous fortune, ",
      "Or to take arms against a sea of troubles, ");

    // 创建管道。
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).create();
    Pipeline p = Pipeline.create(options);

    // 应用Create，传递列表和编码器，来创建PCollection。
    p.apply(Create.of(LINES)).setCoder(StringUtf8Coder.of());
}
```
```py
with beam.Pipeline(options=pipeline_options) as p:

  lines = (p
           | beam.Create([
               'To be, or not to be: that is the question: ',
               'Whether \'tis nobler in the mind to suffer ',
               'The slings and arrows of outrageous fortune, ',
               'Or to take arms against a sea of troubles, ']))
```

### 3.2. PCollection特点 {#pcollection-characteristics}

一个 `PCollection` 由创建它的特定 `Pipeline` 对象拥有;多个管道无法共享一个 `PCollection`。在某些方面，一个 `PCollection` 的功能类似于集合类。然而，一个 `PCollection` 可能在几个关键方面有所不同：

#### 3.2.1. 元素类型 {#element-type}

一个 `PCollection` 的元素可以是任何类型，但必须都是相同的类型。 然而，为了支持分布式处理，Beam需要能够将每个单独的元素编码为字节串（这样元素就可以传递给分布式工作者）。Beam SDKs提供了一种数据编码机制，其中包括对常用类型的内置编码，以及支持根据需要指定自定义编码。

#### 3.2.2. 不变性 {#immutability}

一个 `PCollection` 是不可变的。一旦创建，你就不能添加、删除或更改单个元素。  一个Beam变换可以处理 `PCollection` 中的每个元素并生成新的管道数据（作为新的`PCollection`），*但它不会消耗或修改原始输入集合*。

#### 3.2.3. 随机访问 {#random-access}

一个 `PCollection` 不支持对单个元素的随机访问。 相反，Beam转换会单独考虑 `PCollection` 中的每个元素。

#### 3.2.4. 大小和有界性 {#size-and-boundedness}

一个 `PCollection` 是一个大的、不可变的元素“袋”。一个 `PCollection` 可以包含多少个元素数量上没有上限;  任何给定的 `PCollection` 都可能适合存储在单个机器上的内存中，或者它可能代表由持久数据存储支持的非常大的分布式数据集。

一个 `PCollection` 的大小可以是**有界的**，也可以是**无界的**。 一个**有界**的 `PCollection` 表示已知固定大小的数据集，而**无界** `PCollection` 表示无限大小的数据集。一个 `PCollection` 是有界的还是无界的取决于它所表示的数据集的源。. 从批处理数据源（例如文件或数据库）读取数据会创建一个有界 `PCollection`。  从流式传输或不断更新的数据源（如发布/订阅或Kafka）中读取，会创建一个无界的 `PCollection`（除非您明确告诉它不要这样做）。

您的 `PCollection` 的有界（或无界）性质会影响Beam处理数据的方式。可以使用批处理作业处理一个有界 `PCollection`，批处理作业可以一次读取整个数据集，并在有限长度的作业中执行处理。必须使用连续运行的流式作业处理无界 `PCollection` ，因为在任何时候都不能对整个集合进行处理。

Beam使用[窗口](#windowing)将一个不断更新的无界 `PCollection` 划分为有限大小的逻辑窗口。这些逻辑窗口由与数据元素相关联的某些特征确定，例如**时间戳**。聚合转换（例如 `GroupByKey` 和 `Combine` ）在每个窗口的基础上工作 —当生成数据集时，他们将每个 `PCollection` 作为这些有限窗口的连续处理。


#### 3.2.5. 元素时间戳 {#element-timestamps}

在一个 `PCollection` 中的 每个元素都有一个关联的固有**时间戳**。 每个元素的时间戳最初由创建 `PCollection` 的[Source](#pipeline-io)分配。 创建无界 `PCollection` 的源通常会为每个新元素分配一个时间戳，该时间戳对应于读取或添加元素的时间。

> **注意**: 为固定数据集创建有界 `PCollection` 的源也会自动分配时间戳，但最常见的行为是为每个元素分配相同的时间戳（`Long.MIN_VALUE`）。

时间戳对于一个包含具有固有时间概念的元素的 `PCollection` 非常有用。如果您的管道正在读取事件流（如Tweets或其他社交媒体消息），那么每个元素可能会使用事件发布的时间作为元素时间戳。

如果数据源不为您执行此操作，您可以手动将时间戳分配给一个 `PCollection` 的元素。 如果元素有一个固有的时间戳，则您希望这样做，但时间戳位于元素本身的结构中（例如服务器日志条目中的“time”字段）的某个位置。Beam具有将一个 `PCollection` 作为输入和输出的[变换](#transforms)，该转换与附加了时间戳的 `PCollection` 相同; 有关如何执行此操作的详细信息，请参阅[添加时间戳](#adding-timestamps-to-a-pcollections-elements)。

## 4. 变换 {#transforms}

变换是您管道中的操作，并提供一个通用的处理框架。  您以函数对象的形式提供处理逻辑（通俗地称为“用户代码”），并且您的用户代码应用于输入 `PCollection`（或多个 `PCollection` ）的每个元素。
根据您选择的管道运行程序和后端，集群中的许多不同工作者可以并行执行用户代码的实例。在每个工作者上运行的用户代码生成输出元素，这些元素最终被添加到变换产生的最终输出 `PCollection` 中。

Beam SDK包含许多不同的变换，您可以将这些变换应用于管道的 `PCollection`。 这些变换包括通用核心变换，例如[ParDo](#pardo)或[Combine](#combine)。 SDK中还包含预先编写的[组合变换](#composite-transforms)，它们以一种有用的处理模式组合一个或多个核心变换，例如计算或组合集合中的元素。 您还可以定义自己更复杂的组合变换，以适应管道的确切用例。

### 4.1. 应用变换 {#applying-transforms}

要调用一个变换，必须将它**应用**于输入 `PCollection`。Beam SDK中的每个转换都有一个通用的 `apply` 方法（或管道运算符 `|`）。 调用多个Beam变换类似于*方法链接*，但有一点不同：将变换应用于输入 `PCollection` 时，将变换本身作为参数传递，然后操作返回输出 `PCollection`。它的一般形式如下：

```java
[Output PCollection] = [Input PCollection].apply([Transform])
```
```py
[Output PCollection] = [Input PCollection] | [Transform]
```

因为Beam对 `PCollection` 使用了一个通用的 `apply` 方法，所以您既可以按顺序链接变换，也可以应用包含嵌套在其中的其他变换的变换（在Beam SDK中称为[组合变换](#composite-transforms)）。

您如何应用您管道的转换决定了您管道的结构。思考您管道的最佳方法是将其看作一个有向无环图，其中节点是 `PCollection`，边是变换。例如，您可以链接变换来创建一个顺序的管道，如下所示:

```java
[Final Output PCollection] = [Initial Input PCollection].apply([First Transform])
.apply([Second Transform])
.apply([Third Transform])
```
```py
[Final Output PCollection] = ([Initial Input PCollection] | [First Transform]
              | [Second Transform]
              | [Third Transform])
```

上面的管道生成的工作流程图如下所示。
<img src="https://beam.apache.org/images/design-your-pipeline-linear.png" alt="此线性管道以一个输入集合开始，顺序应用三个变换，并以一个输出集合结束。">

*图：具有三个顺序变换的线性管道.*

但是，请注意，转换*不会消耗或以其他方式更改*输入集合 - 请记住，一个 `PCollection` 根据定义是不可变的。 这意味着您可以将多个变换应用于同一个输入 `PCollection` 来创建分支管道，如下所示： 

```java
[PCollection of database table rows] = [Database Table Reader].apply([Read Transform])
[PCollection of 'A' names] = [PCollection of database table rows].apply([Transform A])
[PCollection of 'B' names] = [PCollection of database table rows].apply([Transform B])
```
```py
[PCollection of database table rows] = [Database Table Reader] | [Read Transform]
[PCollection of 'A' names] = [PCollection of database table rows] | [Transform A]
[PCollection of 'B' names] = [PCollection of database table rows] | [Transform B]
```

上面的分支管道生成的工作流程图如下所示。

<img src="https://beam.apache.org/images/design-your-pipeline-multiple-pcollections.png" alt="此管道将两个变换应用于单个输入集合。 每个转换都会生成输出集合。">

*图：一个分支管道。 将两个转换应用于数据库表行的单个PCollection。*

您还可以构建您自己的[组合转换](#composite-transforms)，将多个子步骤嵌套在一个更大的转换中。组合转换对于构建可重用的简单步骤序列特别有用，这些步骤可以在许多不同的地方使用。

### 4.2. 核心Beam变换 {#core-beam-transforms}

Beam provides the following core transforms, each of which represents a different
processing paradigm:

* `ParDo`
* `GroupByKey`
* `CoGroupByKey`
* `Combine`
* `Flatten`
* `Partition`

#### 4.2.1. ParDo {#pardo}

`ParDo` 是一种用于通用并行处理的Beam变换。`ParDo` 处理范例类似于Map/Shuffle/Reduce风格算法的"Map"阶段：一个 `ParDo` 变换考虑输入 `PCollection` 中的每个元素，对该元素执行一些处理功能（您的用户代码），并发出零 ，一个或多个元素到输出 `PCollection`。

`ParDo` 对于各种常见的数据处理操作非常有用，包括：

* **过滤数据集。** 您可以使用 `ParDo` 考虑  `PCollection` 中的每个元素，并将该元素输出到一个新集合，或将它丢弃。
* **格式化或类型转换数据集中的每个元素。** 如果您的输入 `PCollection` 包含的元素类型或格式与您想要的不同，则可以使用 `ParDo` 对每个元素执行转换，并将结果输出到一个新的 `PCollection`。
* **提取数据集中每个元素的部分。** 例如，如果您有一个包含多个字段的 `PCollection` 记录， 则可以使用 `ParDo` 将要考虑的字段解析为一个新的 `PCollection`。
* **对数据集中的每个元素执行计算。** 您可以使用 `ParDo` 对 `PCollection` 的每个元素或某些元素执行简单或复杂的计算，并将结果输出为新的 `PCollection`。

在这些角色中，`ParDo` 是管道中常见的中间步骤。您可以使用它从一组原始输入记录中提取某些字段，或将原始输入转换为其他格式;您也可以使用 `ParDo` 将处理后的数据转换为适合输出的格式，如数据库表行或可打印字符串。

当您应用一个 `ParDo` 转换时，您需要以 `DoFn` 对象的形式提供用户代码。 `DoFn` 是一个Beam SDK类，它定义了分布式处理函数。

>当您创建 `DoFn` 的子类时, 请注意，您的子类应该遵守[为Beam变换编写用户代码的要求](#requirements-for-writing-user-code-for-beam-transforms).

##### 4.2.1.1. 应用ParDo {#applying-pardo}

与所有Beam变换一样，您通过在输入 `PCollection` 上调用 `apply` 方法来应用 `ParDo`，
并将 `ParDo` 作为参数传递，如下面的示例代码所示:

```java
// 字符串的输入PCollection。
PCollection<String> words = ...;

// DoFn对输入PCollection中的每个元素执行。
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }

// 将ParDo应用于PCollection“words”，来计算每个单词的长度。
PCollection<Integer> wordLengths = words.apply(
    ParDo
    .of(new ComputeWordLengthFn()));        // DoFn对我们在上面定义的每个元素执行。

```
```py
# 字符串的输入PCollection。
words = ...

# DoFn对输入PCollection中的每个元素执行。
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]

# 将ParDo应用于PCollection“words”,来计算每个单词的长度。
word_lengths = words | beam.ParDo(ComputeWordLengthFn())
```

```go
// 字符串的输入PCollection。
var words beam.PCollection = ...

func computeWordLengthFn(word string) int {
      return len(word)
}

wordLengths := beam.ParDo(s, computeWordLengthFn, words)
```

在示例中，我们的输入 `PCollection` 包含 `String` 值。 我们应用一个 `ParDo` 变换，它指定一个函数（`ComputeWordLengthFn`）来计算每个字符串的长度，并将结果输出到一个存储每个单词长度的新 `Integer` 值 `PCollection`。 
##### 4.2.1.2. 创建一个DoFn

传递给 `ParDo` 的 `DoFn` 对象包含应用于输入集合中的元素的处理逻辑。您将编写的最重要的代码片段通常是这些 `DoFn`——它们定义了管道的确切数据处理任务。.

> **注意:** 当您创建您的 `DoFn` 时，请注意[为Beam变换编写用户代码的要求](#requirements-for-writing-user-code-for-beam-transforms)，并确保您的代码遵循这些要求。


一个 `DoFn` 每次处理输入 `PCollection` 中的一个元素。 当您在创建 `DoFn` 的子类时，您需要提供与输入和输出元素的类型匹配的类型参数。如果您的 `DoFn` 处理传入的 `String` 元素并为输出集合生成 `Integer` 元素（就像我们之前的示例`ComputeWordLengthFn`），您的类声明将如下所示：

```java
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }
```

在您的 `DoFn` 子类中, 您将编写一个带有 `@ProcessElement` 注解的方法，在这个方法中，您可以提供实际的处理逻辑。您不需要手动从输入集合中提取元素;Beam SDKs会为您处理。您的 `@ProcessElement` 方法应该接受一个带有 `@Element` 标记的参数，该参数将使用input元素填充。为了输出元素，该方法还可以采用 `OutputReceiver`类型的参数，该参数提供用于发出元素的方法。参数类型必须匹配 `DoFn` 的输入和输出类型，否则框架将引发错误。 注意：@Llement和OutputReceiver是在Beam 2.5.0中引入的;如果使用早期版本的Beam，则应使用ProcessContext参数。


在您的 `DoFn` 子类中，您将编写一个方法 `process`，您可以在其中提供实际的处理逻辑。 您不需要从输入集合中手动提取元素; Beam SDKs会为您处理。 您的 `process` 方法应该接受 `element` 类型的对象。 这是输入元素，并使用 `yield` 或 `return` 语句在 `process` 方法内部发出输出。

```java
static class ComputeWordLengthFn extends DoFn<String, Integer> {
  @ProcessElement
  public void processElement(@Element String word, OutputReceiver<Integer> out) {
    // 使用OutputReceiver.output发出输出元素。
    out.output(word.length());
  }
}
```
```py
class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [len(element)]
```

> **注意：** 如果输入 `PCollection`中的元素是键/值对，则可以分别使用 `element.getKey()` 或 `element.getValue()` 来访问键或值。

通常会调用给定的 `DoFn` 实例一次或多次来处理某些任意元素束。但是，Beam不保证确切的调用次数;可以在给定的工作节点上多次调用它以解决故障和重试。因此，您可以在处理方法的多个调用之间缓存信息，但是如果这样做，请确保实现**不依赖于调用的次数**。

在您的处理方法中，您还需要满足一些不变性要求，以确保Beam和处理后端可以安全地序列化并缓存管道中的值。 您的方法应符合以下要求：

* 您不应以任何方式修改 `@Element` 注解或 `ProcessContext.sideInput()` (输入集合中的传入元素)返回的元素.
* 一旦使用 `OutputReceiver.output()` 输出了一个值，就不应该以任何方式修改该值。

##### 4.2.1.3. 轻量级DoFns和其他抽象 {#lightweight-dofns}

如果你的函数相对简单，你可以通过提供一个内联的轻量级 `DoFn` 来简化你对 `ParDo` 的使用，作为一个lambda函数的匿名内部类实例。

下面是一个前面的例子中带有 `ComputeLengthWordsFn` 的 `ParDo`示例，其中 `DoFn` 被指定为匿名内部类实例lambda函数：

```java
//输入PCollection。
PCollection<String> words = ...;

//将带有匿名DoFn的ParDo应用于PCollection单词。
//将结果保存为PCollection wordLengths。
PCollection<Integer> wordLengths = words.apply(
  "ComputeWordLengths",                     
  ParDo.of(new DoFn<String, Integer>() {    //变换将一个DoFn命名为匿名内部类实例
      @ProcessElement
      public void processElement(@Element String word, OutputReceiver<Integer> out) {
        out.output(word.length());
      }
    }));
```
```py
# 输入PCollection的字符串。
words = ...

# 将lambda函数应用于PCollection单词。
# 将结果保存为PCollection wordLengths。
word_lengths = words | beam.FlatMap(lambda word: [len(word)])
```
```go
//单词是字符串的输入PCollection
var words beam.PCollection = ...

lengths := beam.ParDo(s, func (word string) int {
      return len(word)
}, words)
```

如果您的 `ParDo` 执行输入元素与输出元素的一对一映射 - 也就是说，对于每个输入元素，它应用一个*恰好生成一个*输出元素的函数，则可以使用更高级别的 `MapElements` `Map` 变换。 为了更加简洁， `MapElements` 可以接受匿名的Java 8 lambda函数。

这是使用 `MapElements` `Map` 的前一个示例：

```java
//输入PCollection。
PCollection<String> words = ...;

//将带有匿名lambda函数的MapElements应用于PCollection words。
//将结果保存为PCollection wordLengths。
PCollection<Integer> wordLengths = words.apply(
  MapElements.into(TypeDescriptors.integers())
             .via((String word) -> word.length()));
```
```py
#输入PCollection的字符串。
words = ...

# 将带有lambda函数的Map应用于PCollection单词。
# 将结果保存为PCollection wordLengths。
word_lengths = words | beam.Map(len)
```
> **注意：** 您可以将Java 8 lambda函数与其他几个Beam变换一起使用，包括 `Filter`, `FlatMapElements`, 和 `Partition`。

#### 4.2.2. GroupByKey {#groupbykey}

`GroupByKey`是用于处理键/值对集合的Beam变换。 这是一个并行归约操作，类似于Map/Shuffle/Reduce风格算法的Shuffle阶段。`GroupByKey` 的输入是表示*多映射*的键/值对的集合，其中集合包含多个具有相同键但不具有相同值的对。给定这样一个集合，您可以使用   `GroupByKey`收集与每个惟一键关联的所有值。

`GroupByKey` 是一种聚合某些有共同点的数据的好方法。例如，如果您有一个存储客户订单记录的集合，您可能希望将来自相同邮政编码的所有订单组合在一起（其中键/值对的“键”是邮政编码字段，并且“ 值“是记录的剩余部分）。

让我们用一个简单的例子来研究 `GroupByKey` 的机制，其中我们的数据集由文本文件中的单词和它们出现的行号组成。 我们希望将共享同一个单词（键）的所有行号（值）组合在一起，让我们看到文本中某个特定单词出现的所有位置。

我们的输入是键/值对的 `PCollection`，其中每个单词是一个键，值是文件出现的文件中的行号。下面是输入集合中的键/值对列表:
```
cat, 1
dog, 5
and, 1
jump, 3
tree, 2
cat, 5
dog, 2
and, 2
cat, 9
and, 6
...
```

`GroupByKey` 使用相同的键收集所有值，并输出一个新的对，该对由唯一键和输入集合中与该键关联的所有值的集合组成。 如果我们将 `GroupByKey` 应用于上面的输入集合，则输出集合将如下所示：

```
cat, [1,5,9]
dog, [5,2]
and, [1,2,6]
jump, [3]
tree, [2]
...
```

因此，`GroupByKey` 表示从多映射（多个键到单个值）到单一映射（值集合的唯一键）的变换。

##### 4.2.2.1 GroupByKey和无界PCollections {#groupbykey-and-unbounded-pcollections}

如果您使用无界 `PCollection`，则必须使用[非全局窗口](#setting-your-pcollections-windowing-function)或[聚合触发器](#triggers)才能执行 `GroupByKey` 或[CoGroupByKey](#cogroupbykey)。 这是因为有界 `GroupByKey` 或 `CoGroupByKey` 必须等待收集某个键的所有数据，但是对于无界集合，数据是无限的。窗口和/或触发器允许对无界数据流中的逻辑、有限的数据束进行分组操作。

如果您将 `GroupByKey` 或 `CoGroupByKey` 应用于一组无界 `PCollection` ，而没有为每个集合设置非全局窗口策略，触发策略或同时设置这两种策略，则Beam会在管道构建时生成IllegalStateException错误。

使用 `GroupByKey` 或 `CoGroupByKey` 对应用了[窗口策略](#windowing)的 `PCollection` 进行分组时，要分组的所有 `PCollection` *必须使用相同的窗口策略*和窗口大小。 例如，您要合并的所有集合必须使用（假设）相同的5分钟固定窗口，或每30秒开始的一次4分钟滑动窗口。

如果您的管道尝试使用 `GroupByKey` 或 `CoGroupByKey` 将 `PCollection`与不兼容的窗口合并，则Beam会在管道构建时生成IllegalStateException错误。

#### 4.2.3. CoGroupByKey {#cogroupbykey}

`CoGroupByKey` 执行具有相同键类型的两个或多个键/值 `PCollection` 的关系连接。[设计您的管道](https://beam.apache.org/documentation/pipelines/design-your-pipeline/#multiple-sources)显示了一个使用连接的示例管道。

如果您有多个数据集来提供有关相关事物的信息，请考虑使用`CoGroupByKey`。例如，假设您有两个不同的用户数据文件：一个文件有名称和电子邮件地址; 另一个文件有姓名和电话号码。 您可以使用用户名作为公共键，并将其他数据作为关联值，来连接这两个数据集。 在连接之后，您有一个数据集，其中包含与每个名称关联的所有信息（电子邮件地址和电话号码）。

如果您使用的是无界 `PCollection`，则必须使用[非全局窗口](#setting-your-pcollections-windowing-function)或[聚合触发器](#triggers)才能执行一个 `CoGroupByKey`。有关详细信息，请参阅[GroupByKey和无界PCollections](#groupbykey-and-unbounded-pcollections)。


在Java的Beam SDK中，`CoGroupByKey` 接受一个以 `PCollection` 为键的元组 (`PCollection<KV<K, V>>`)作为输入。为了类型安全，SDK要求您将每个 `PCollection` 作为 `KeyedPCollectionTuple` 的一部分传递。您必须为要传递给 `CoGroupByKey` 的 `KeyedPCollectionTuple` 中的每个输入 `PCollection` 声明一个  `TupleTag`。作为输出，`CoGroupByKey` 返回一个 `PCollection<KV<K, CoGbkResult>>`，它根据所有输入 `PCollection` 的公共键对值进行分组。 每个键（所有类型 `K`）将具有不同的 `CoGbkResult`，这是从 `TupleTag<T>` 到 `Iterable<T>`的映射。您可以使用一个随初始集合提供的 `TupleTag` 来访问 `CoGbkResult` 对象中的特定集合。

在Python的Beam SDK中， `CoGroupByKey` 接受一个以 `PCollection` 为键的字典作为输入。作为输出，`CoGroupByKey` 创建单个输出 `PCollection`，这个输出的 `PCollection` 为输入 `PCollection` 中的每个键包含了一个键/值元组。每个键的值是一个字典，它将每个标记映射到对应 `PCollection` 中键下值的一个迭代。

以下概念示例使用两个输入集合来显示 `CoGroupByKey` 的机制。

第一组数据有一个名为 `emailsTag` 的 `TupleTag<String>`，包含名称和电子邮件地址。第二组数据有一个名为 `phonesTag` 的 `TupleTag<String>`，包含名称和电话号码。 第一组数据包含名称和电子邮件地址。 第二组数据包含姓名和电话号码。

```java
final List<KV<String, String>> emailsList =
    Arrays.asList(
        KV.of("amy", "amy@example.com"),
        KV.of("carl", "carl@example.com"),
        KV.of("julia", "julia@example.com"),
        KV.of("carl", "carl@email.com"));

final List<KV<String, String>> phonesList =
    Arrays.asList(
        KV.of("amy", "111-222-3333"),
        KV.of("james", "222-333-4444"),
        KV.of("amy", "333-444-5555"),
        KV.of("carl", "444-555-6666"));

PCollection<KV<String, String>> emails = p.apply("CreateEmails", Create.of(emailsList));
PCollection<KV<String, String>> phones = p.apply("CreatePhones", Create.of(phonesList));
```

在 `CoGroupByKey` 之后，结果数据包含与来自任何输入集合的每个唯一键相关联的所有数据。

```py
emails_list = [
    ('amy', 'amy@example.com'),
    ('carl', 'carl@example.com'),
    ('julia', 'julia@example.com'),
    ('carl', 'carl@email.com'),
]
phones_list = [
    ('amy', '111-222-3333'),
    ('james', '222-333-4444'),
    ('amy', '333-444-5555'),
    ('carl', '444-555-6666'),
]

emails = p | 'CreateEmails' >> beam.Create(emails_list)
phones = p | 'CreatePhones' >> beam.Create(phones_list)
```

```java
final TupleTag<String> emailsTag = new TupleTag<>();
final TupleTag<String> phonesTag = new TupleTag<>();

final List<KV<String, CoGbkResult>> expectedResults =
    Arrays.asList(
        KV.of(
            "amy",
            CoGbkResult.of(emailsTag, Arrays.asList("amy@example.com"))
                .and(phonesTag, Arrays.asList("111-222-3333", "333-444-5555"))),
        KV.of(
            "carl",
            CoGbkResult.of(emailsTag, Arrays.asList("carl@email.com", "carl@example.com"))
                .and(phonesTag, Arrays.asList("444-555-6666"))),
        KV.of(
            "james",
            CoGbkResult.of(emailsTag, Arrays.asList())
                .and(phonesTag, Arrays.asList("222-333-4444"))),
        KV.of(
            "julia",
            CoGbkResult.of(emailsTag, Arrays.asList("julia@example.com"))
                .and(phonesTag, Arrays.asList())));
```
```py
results = [
    ('amy', {
        'emails': ['amy@example.com'],
        'phones': ['111-222-3333', '333-444-5555']}),
    ('carl', {
        'emails': ['carl@email.com', 'carl@example.com'],
        'phones': ['444-555-6666']}),
    ('james', {
        'emails': [],
        'phones': ['222-333-4444']}),
    ('julia', {
        'emails': ['julia@example.com'],
        'phones': []}),
]
```

下面的代码示例,将两个 `PCollection` 与 `CoGroupByKey` 连接，然后使用 `ParDo` 来消费结果。 然后，代码使用标签来查找并格式化每个集合中的数据。

```java
PCollection<KV<String, CoGbkResult>> results =
    KeyedPCollectionTuple.of(emailsTag, emails)
        .and(phonesTag, phones)
        .apply(CoGroupByKey.create());

PCollection<String> contactLines =
    results.apply(
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<String, CoGbkResult> e = c.element();
                String name = e.getKey();
                Iterable<String> emailsIter = e.getValue().getAll(emailsTag);
                Iterable<String> phonesIter = e.getValue().getAll(phonesTag);
                String formattedResult =
                    Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
                c.output(formattedResult);
              }
            }));
```
```py
# 结果PCollection包含输入PCollections中每个键的一个键值元素。
# 该对中的键是输入的键，并且值是包含两个条目的字典： 
#'emails' - emails的PCollection中当前键的所有值的可迭代以及
#'phones': phones的PCollection中当前键的所有值的可迭代。.
results = ({'emails': emails, 'phones': phones}
           | beam.CoGroupByKey())

def join_info(name_info):
  (name, info) = name_info
  return '%s; %s; %s' %\
      (name, sorted(info['emails']), sorted(info['phones']))

contact_lines = results | beam.Map(join_info)
```

The formatted data looks like this:

```java
final List<String> formattedResults =
    Arrays.asList(
        "amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
        "carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
        "james; []; ['222-333-4444']",
        "julia; ['julia@example.com']; []");
```
```py
formatted_results = [
    "amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
    "carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
    "james; []; ['222-333-4444']",
    "julia; ['julia@example.com']; []",
]

```

#### 4.2.4. 组合 {#combine}

[`Combine`](https://beam.apache.org/releases/javadoc/2.13.0/index.html?org/apache/beam/sdk/transforms/Combine.html) [`Combine`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)
是一种Beam变换，用于组合数据中的元素或值集合。`Combine` 有对整个 `PCollection` 起作用的变体，还有一些变体组合了 `PCollection` 键/值对中每个键的值。

当您应用 `Combine` 变换时，必须提供包含组合元素或值的逻辑的函数。 组合函数应该是可交换的和关联的，因为对于给定键的所有值，函数不一定都只调用一次。 由于输入数据（包括值集合）可以分布在多个工作者之间，所以可以多次调用组合函数来对值集合的子集执行部分组合。 Beam SDK还为常见的数字组合操作（如sum，min和max）提供了一些预构建的组合函数。

简单的组合操作（例如求和）通常可以作为一个简单的函数来实现。更复杂的组合操作可能需要您创建 `CombineFn`  的子类，它具有与输入/输出类型不同的累积类型。

##### 4.2.4.1. 使用简单函数的简单组合 {#simple-combines}

以下示例代码显示了一个简单的组合函数。

```java
//对一组Integer值求和。 函数SumInts实现了SerializableFunction接口。
public static class SumInts implements SerializableFunction<Iterable<Integer>, Integer> {
  @Override
  public Integer apply(Iterable<Integer> input) {
    int sum = 0;
    for (int item : input) {
      sum += item;
    }
    return sum;
  }
}
```

```py
pc = [1, 10, 100, 1000]

def bounded_sum(values, bound=500):
  return min(sum(values), bound)
small_sum = pc | beam.CombineGlobally(bounded_sum)              # [500]
large_sum = pc | beam.CombineGlobally(bounded_sum, bound=5000)  # [1111]

```

##### 4.2.4.2. 使用CombineFn的高级组合 {#advanced-combines}

对于更复杂的组合函数，您可以定义 `CombineFn` 的子类。 如果组合函数需要更复杂的累加器，必须执行额外的预处理或后处理，可能会更改输出类型或者把键考虑在内，则您应该使用`CombineFn`。

一般的组合操作包括四个操作。 在创建 `CombineFn` 的子类时，必须通过重写相应的方法来提供四个操作：

1. **创建累加器** 创建一个新的“本地”累加器。在这个例子中，取一个平均值，一个本地累加器跟踪正在运行的值的和(最后的平均除法的分子值))和到目前为止求和的值的数量（分母值）。它可以以分布式方式被调用任意次数。

2. **添加输入** 将一个输入元素添加到累加器，返回累加器值。在我们的示例中，它将更新求总和并增加计数。 它也可以并行调用。

3. **合并累加器** 将几个累加器合并为一个累加器;  这是在最终计算之前如何将多个累加器中的数据组合的方式。在平均值计算的情况下，表示除法的每个部分的累加器被合并在一起。 可以在它的输出上再次调用它。

4. **提取输出** 执行最终计算。 在计算平均值的情况下，这意味着将所有值的组合总和除以求和值的数量。它只在最终合并的累加器上调用一次。

以下示例代码显示如何定义一个计算平均值的 `CombineFn`：

```java
public class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
  public static class Accum {
    int sum = 0;
    int count = 0;
  }

  @Override
  public Accum createAccumulator() { return new Accum(); }

  @Override
  public Accum addInput(Accum accum, Integer input) {
      accum.sum += input;
      accum.count++;
      return accum;
  }

  @Override
  public Accum mergeAccumulators(Iterable<Accum> accums) {
    Accum merged = createAccumulator();
    for (Accum accum : accums) {
      merged.sum += accum.sum;
      merged.count += accum.count;
    }
    return merged;
  }

  @Override
  public Double extractOutput(Accum accum) {
    return ((double) accum.sum) / accum.count;
  }
}
```
```py
pc = ...
class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0)

  def add_input(self, sum_count, input):
    (sum, count) = sum_count
    return sum + input, count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum / count if count else float('NaN')
```

如果你要组合一个键值对的 `PCollection`，则每个[键组合](#combining-values-in-a-keyed-pcollection)通常就足够了。如果您需要根据键更改组合策略（例如，某些用户为MIN，其他用户为MAX），则可以定义 `KeyedCombineFn` 来访问组合策略中的键。

##### 4.2.4.3. 将PCollection组合为单个值{#combining-pcollection}

使用全局组合将给定 `PCollection` 中的所有元素转换为单个值，在管道中表示为包含一个元素的新 `PCollection`。 以下示例代码显示如何应用Beam提供的求和组合函数，以便为整数的  `PCollection` 生成单个求和值。

```java
// Sum.SumIntegerFn()组合输入PCollection中的元素。 生成的PCollection（称为sum）它包含一个值：输入PCollection中所有元素的总和。
PCollection<Integer> pc = ...;
PCollection<Integer> sum = pc.apply(
   Combine.globally(new Sum.SumIntegerFn()));
```
```py
# sum组合了输入PCollection中的元素。
# 生成的PCollection（称为result）它包含一个值：输入PCollection中所有元素的总和。
pc = ...
average = pc | beam.CombineGlobally(AverageFn())
```

##### 4.2.4.4. 组合和全局窗口{#combine-global-windowing}

如果您的输入 `PCollection` 使用默认的全局窗口，则默认行为是返回包含一个项的 `PCollection`。 该项的值来自您在应用 `Combine` 时指定的组合函数中的累加器。 例如，Beam提供的sum组合函数返回一个零值（一个空输入的求和），而min组合函数返回最大值或无穷大值。

要使 `Combine` 在输入为空时返回空的 `PCollection`，请在您应用的 `Combine` 变换时指定 `.withoutDefaults` ，如下面的代码示例所示：

```java
PCollection<Integer> pc = ...;
PCollection<Integer> sum = pc.apply(
  Combine.globally(new Sum.SumIntegerFn()).withoutDefaults());
```
```py
pc = ...
sum = pc | beam.CombineGlobally(sum).without_defaults()
```

##### 4.2.4.5. 组合和非全局窗口 {#combine-non-global-windowing}

如果您的 `PCollection` 使用任何非全局窗口函数，则Beam不会提供默认行为。 应用 `Combine` 时，您必须指定以下选项之一：
* 指定 `.withoutDefaults`，其中在输入 `PCollection` 中为空的窗口，将在输出集合中同样为空。
* 指定 `.asSingletonView`，其中输出立即转换为 `PCollectionView`，当用作侧输入时，它将为每个空窗口提供默认值。 如果您管道的 `Combine` 结果在后面的管道中用作侧输入时，才需要使用这个选项。

##### 4.2.4.6.在一个键PCollection中组合值 {#combining-values-in-a-keyed-pcollection}

在创建一个键PCollection（例如，通过使用 `GroupByKey` 变换）之后，一个常见模式是将与每个键关联的值集合组合成单个合并值。 借鉴 `GroupByKey` 中的上一个示例，一个名为 `groupedWords` 的键分组 `PCollection` 如下所示：
```
  cat, [1,5,9]
  dog, [5,2]
  and, [1,2,6]
  jump, [3]
  tree, [2]
  ...
```

在上面的 `PCollection` 中，每个元素都有一个字符串键（例如，“cat”）和一个可迭代的整数值（在第一个元素中，包含[1,5,9]）。 如果我们管道的下一个处理步骤组合了这些值（而不是单独考虑它们），您可以组合可迭代的整数来创建一个单个的，合并的值，以便与每个键配对。这种之后合并值集合 `GroupByKey` 模式，相当于Beam的Combine PerKey变换。 您提供给Combine PerKey的组合函数必须是关联归约函数或 `CombineFn` 的一个子类。

```java
// PCollection按键分组，与每个键关联的Double值组合成一个Double。
PCollection<KV<String, Double>> salesRecords = ...;
PCollection<KV<String, Double>> totalSalesPerPerson =
  salesRecords.apply(Combine.<String, Double, Double>perKey(
    new Sum.SumDoubleFn()));

// 组合值的类型与每个键的原始值集合的类型不同。 PCollection具有String类型的键和Integer类型的值，组合值为一个Double。
PCollection<KV<String, Integer>> playerAccuracy = ...;
PCollection<KV<String, Double>> avgAccuracyPerPlayer =
  playerAccuracy.apply(Combine.<String, Integer, Double>perKey(
    new MeanInts())));
```
```py
# PCollection按键分组，与每个键关联的数值平均为一个浮点数。
player_accuracies = ...
avg_accuracy_per_player = (player_accuracies
                           | beam.CombinePerKey(
                               beam.combiners.MeanCombineFn()))
```

#### 4.2.5. 平面 {#flatten}

[`Flatten`](https://beam.apache.org/releases/javadoc/2.13.0/index.html?org/apache/beam/sdk/transforms/Flatten.html) [`Flatten`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py)是用于存储相同数据类型的 `PCollection` 对象的Beam变换。 `Flatten` 将多个 `PCollection` 对象合并为一个逻辑 `PCollection`。

以下示例显示了，如何应用一个  `Flatten` 变换来合并多个 `PCollection`对象。

```java
// Flatten接受给定类型的PCollection对象的PCollectionList。
//返回单个PCollection，其中包含该列表中PCollection对象中的所有元素。
PCollection<String> pc1 = ...;
PCollection<String> pc2 = ...;
PCollection<String> pc3 = ...;
PCollectionList<String> collections = PCollectionList.of(pc1).and(pc2).and(pc3);

PCollection<String> merged = collections.apply(Flatten.<String>pCollections());
```

```py
# Flatten接受一个PCollection对象的元组。
# 返回单个PCollection，其中包含该元组中PCollection对象中的所有元素。
merged = (
    (pcoll1, pcoll2, pcoll3)
    #一个元组列表可以直接“管道化”为Flatten变换。
    | beam.Flatten())
```

##### 4.2.5.1. 在合并集合中的数据编码 {#data-encoding-merged-collections}

默认情况下，输出 `PCollection` 的编码器与输入  `PCollectionList` 中第一个 `PCollection`的编码器相同。 但是，输入的 `PCollection` 对象可以使用不同的编码器，只要它们都包含您选择的语言中相同的数据类型即可。

##### 4.2.5.2. 合并窗口集合 {#merging-windowed-collections}

当使用 `Flatten` 合并应用了窗口策略的 `PCollection` 对象时，要合并的所有 `PCollection` 对象必须使用兼容的窗口策略和窗口大小。 例如，您正在合并的所有集合必须全部使用（假设）相同的5分钟固定窗口或每30秒开始一次的4分钟滑动窗口。

如果您的管道尝试使用 `Flatten` 将 `PCollection` 对象与不兼容的窗口合并，则在构建管道时，Beam会生成一个  `IllegalStateException` 错误。

#### 4.2.6. 分区 {#partition}

[`Partition`](https://beam.apache.org/releases/javadoc/2.13.0/index.html?org/apache/beam/sdk/transforms/Partition.html) [`Partition`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py) 是存储相同数据类型的 `PCollection` 对象的Beam变换。`Partition` 将单个 `PCollection` 拆分为固定数量的较小集合。

`Partition` 根据您提供的分区函数划分一个 `PCollection` 的元素。 分区函数包含确定如何将输入 `PCollection` 的元素拆分为每个结果分区 `PCollection` 的逻辑。 必须在图构建时确定分区数。 例如，您可以在运行时将分区数作为命令行选项传递（它之后将被用于构建管道图），但是您无法确定管道中间的分区数（例如，根据您管道图构建后计算的数据）。

下面的示例将 `PCollection`划分为一个百分比组。

```java
// 提供一个int值，其中包含所需的结果分区数量，以及一个表示分区函数的PartitionFn。

// 分区函数。在这个例子中，我们在线定义PartitionFn。返回一个PCollectionList，其中包含作为单个PCollection对象的每个结果分区。
PCollection<Student> students = ...;
//按百分比将学生分成10个分区：
PCollectionList<Student> studentsByPercentile =
    students.apply(Partition.of(10, new PartitionFn<Student>() {
        public int partitionFor(Student student, int numPartitions) {
            return student.getPercentile()  // 0..99
                 * numPartitions / 100;
        }}));

// 您可以使用get方法从PCollectionList中提取每个分区，如下所示：
PCollection<Student> fortiethPercentile = studentsByPercentile.get(4);
```
```py
# 提供一个int值，其中包含所需的结果分区数量，以及一个分区函数(本例中为partition_fn)。
# 返回一个PCollection对象的元组，其中包含作为单个PCollection对象的每个结果分区。
students = ...
def partition_fn(student, num_partitions):
  return int(get_percentile(student) * num_partitions / 100)

by_decile = students | beam.Partition(partition_fn, 10)

# 您可以从PCollection对象的元组中提取每个分区，如下所示：
fortieth_percentile = by_decile[4]
```

### 4.3. 为Beam变换编写用户代码的要求 {#requirements-for-writing-user-code-for-beam-transforms}

当您为一个Beam变换构建用户代码时，应记住执行的分布式特性。 例如，可能有许多函数的副本在许多不同的机器上并行运行，这些副本可能独立运行，不与任何其他副本通信或共享状态。 根据您为你管道选择的管道运行器和处理后端，您的用户代码函数副本可能被重试或运行多次。 因此，您应该谨慎地在用户代码中包含状态依赖之类的内容。

通常，您的用户代码必须至少满足以下要求：

* 您的函数对象必须是**可序列化的**。
* 您的函数对象必须是**线程兼容**的，并且要注意Beam SDK不是线程安全的。

此外，我们建议你让你的函数对象具有**幂等性**。非幂等函数由Beam支持，但当存在外部副作用时，需要额外的考虑来确保正确性。

> **注意：** 这些要求适用于 `DoFn`（与[ParDo](#pardo)变换一起使用的函数对象），`CombineFn`（与[Combine](#combine)变换一起使用的函数对象）和 `WindowFn`（与[Window](#windowing)变换一起使用的函数对象）的子类。

#### 4.3.1. 可串行化 {#user-code-serializability}

您为一个变换提供的任何函数对象都必须是**完全可序列化**的。 这是因为需要将函数的副本序列化并传输到处理集群中的远程工作程序。 用户代码的基类，如 `DoFn`，`CombineFn` 和 `WindowFn`，已实现 `Serializable`; 但是，您的子类不能添加任何不可序列化的成员。

您应该记住的一些其他可串行化因素是：

* 函数对象中的瞬态字段*不会*传输到工作者实例中，因为它们不会自动序列化。
* 在序列化之前避免加载包含有大量数据的字段。
* 函数对象的各个实例不能共享数据。
* 在函数对象被应用后对它进行的改变将不起作用。
* 使用匿名内部类实例在内联声明函数对象时要小心。 在非静态上下文中，内部类实例将隐式包含指向封闭类和该类的状态的指针。 这个封闭类也将被序列化，因此应用于函数对象本身的相同注意事项也适用于这个外部类。

#### 4.3.2. 线程兼容 {#user-code-thread-compatibility}

Your function object should be thread-compatible. Each instance of your function
object is accessed by a single thread at a time on a worker instance, unless you
explicitly create your own threads. Note, however, that **the Beam SDKs are not
thread-safe**. If you create your own threads in your user code, you must
provide your own synchronization. Note that static members in your function
object are not passed to worker instances and that multiple instances of your
function may be accessed from different threads.

#### 4.3.3. 幂等性 {#user-code-idempotence}

It's recommended that you make your function object idempotent--that is, that it
can be repeated or retried as often as necessary without causing unintended side
effects. Non-idempotent functions are supported, however the Beam model provides
no guarantees as to the number of times your user code might be invoked or retried;
as such, keeping your function object idempotent keeps your pipeline's output
deterministic, and your transforms' behavior more predictable and easier to debug.

### 4.4. 侧输入 {#side-inputs}

In addition to the main input `PCollection`, you can provide additional inputs
to a `ParDo` transform in the form of side inputs. A side input is an additional
input that your `DoFn` can access each time it processes an element in the input
`PCollection`. When you specify a side input, you create a view of some other
data that can be read from within the `ParDo` transform's `DoFn` while procesing
each element.

Side inputs are useful if your `ParDo` needs to inject additional data when
processing each element in the input `PCollection`, but the additional data
needs to be determined at runtime (and not hard-coded). Such values might be
determined by the input data, or depend on a different branch of your pipeline.


#### 4.4.1. 将侧输入传递给ParDo {#side-inputs-pardo}

```java
  // Pass side inputs to your ParDo transform by invoking .withSideInputs.
  // Inside your DoFn, access the side input by using the method DoFn.ProcessContext.sideInput.

  // The input PCollection to ParDo.
  PCollection<String> words = ...;

  // A PCollection of word lengths that we'll combine into a single value.
  PCollection<Integer> wordLengths = ...; // Singleton PCollection

  // Create a singleton PCollectionView from wordLengths using Combine.globally and View.asSingleton.
  final PCollectionView<Integer> maxWordLengthCutOffView =
     wordLengths.apply(Combine.globally(new Max.MaxIntFn()).asSingletonView());


  // Apply a ParDo that takes maxWordLengthCutOffView as a side input.
  PCollection<String> wordsBelowCutOff =
  words.apply(ParDo
      .of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
            // In our DoFn, access the side input.
            int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
            if (word.length() <= lengthCutOff) {
              out.output(word);
            }
          }
      }).withSideInputs(maxWordLengthCutOffView)
  );
```
```py
# Side inputs are available as extra arguments in the DoFn's process method or Map / FlatMap's callable.
# Optional, positional, and keyword arguments are all supported. Deferred arguments are unwrapped into their
# actual values. For example, using pvalue.AsIteor(pcoll) at pipeline construction time results in an iterable
# of the actual elements of pcoll being passed into each process invocation. In this example, side inputs are
# passed to a FlatMap transform as extra arguments and consumed by filter_using_length.
words = ...
{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:model_pardo_side_input
%}

# We can also pass side inputs to a ParDo transform, which will get passed to its process method.
# The first two arguments for the process method would be self and element.

{% github_sample /apache/beam/blob/master/sdks/python/apache_beam/examples/snippets/snippets_test.py tag:model_pardo_side_input_dofn
%}
...
```

#### 4.4.2. 侧输入和窗口 {#side-inputs-windowing}

A windowed `PCollection` may be infinite and thus cannot be compressed into a
single value (or single collection class). When you create a `PCollectionView`
of a windowed `PCollection`, the `PCollectionView` represents a single entity
per window (one singleton per window, one list per window, etc.).

Beam uses the window(s) for the main input element to look up the appropriate
window for the side input element. Beam projects the main input element's window
into the side input's window set, and then uses the side input from the
resulting window. If the main input and side inputs have identical windows, the
projection provides the exact corresponding window. However, if the inputs have
different windows, Beam uses the projection to choose the most appropriate side
input window.

For example, if the main input is windowed using fixed-time windows of one
minute, and the side input is windowed using fixed-time windows of one hour,
Beam projects the main input window against the side input window set and
selects the side input value from the appropriate hour-long side input window.

If the main input element exists in more than one window, then `processElement`
gets called multiple times, once for each window. Each call to `processElement`
projects the "current" window for the main input element, and thus might provide
a different view of the side input each time.

If the side input has multiple trigger firings, Beam uses the value from the
latest trigger firing. This is particularly useful if you use a side input with
a single global window and specify a trigger.

### 4.5. 额外的输出 {#additional-outputs}

While `ParDo` always produces a main output `PCollection` (as the return value
from `apply`), you can also have your `ParDo` produce any number of additional
output `PCollection`s. If you choose to have multiple outputs, your `ParDo`
returns all of the output `PCollection`s (including the main output) bundled
together.

#### 4.5.1. 用于多个输出的标签 {#output-tags}

```java
// To emit elements to multiple output PCollections, create a TupleTag object to identify each collection
// that your ParDo produces. For example, if your ParDo produces three output PCollections (the main output
// and two additional outputs), you must create three TupleTags. The following example code shows how to
// create TupleTags for a ParDo with three output PCollections.

  // Input PCollection to our ParDo.
  PCollection<String> words = ...;

  // The ParDo will filter words whose length is below a cutoff and add them to
  // the main ouput PCollection<String>.
  // If a word is above the cutoff, the ParDo will add the word length to an
  // output PCollection<Integer>.
  // If a word starts with the string "MARKER", the ParDo will add that word to an
  // output PCollection<String>.
  final int wordLengthCutOff = 10;

  // Create three TupleTags, one for each output PCollection.
  // Output that contains words below the length cutoff.
  final TupleTag<String> wordsBelowCutOffTag =
      new TupleTag<String>(){};
  // Output that contains word lengths.
  final TupleTag<Integer> wordLengthsAboveCutOffTag =
      new TupleTag<Integer>(){};
  // Output that contains "MARKER" words.
  final TupleTag<String> markedWordsTag =
      new TupleTag<String>(){};

// Passing Output Tags to ParDo:
// After you specify the TupleTags for each of your ParDo outputs, pass the tags to your ParDo by invoking
// .withOutputTags. You pass the tag for the main output first, and then the tags for any additional outputs
// in a TupleTagList. Building on our previous example, we pass the three TupleTags for our three output
// PCollections to our ParDo. Note that all of the outputs (including the main output PCollection) are
// bundled into the returned PCollectionTuple.

  PCollectionTuple results =
      words.apply(ParDo
          .of(new DoFn<String, String>() {
            // DoFn continues here.
            ...
          })
          // Specify the tag for the main output.
          .withOutputTags(wordsBelowCutOffTag,
          // Specify the tags for the two additional outputs as a TupleTagList.
                          TupleTagList.of(wordLengthsAboveCutOffTag)
                                      .and(markedWordsTag)));
```

```py
# To emit elements to multiple output PCollections, invoke with_outputs() on the ParDo, and specify the
# expected tags for the outputs. with_outputs() returns a DoOutputsTuple object. Tags specified in
# with_outputs are attributes on the returned DoOutputsTuple object. The tags give access to the
# corresponding output PCollections.

results = (words | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x')
           .with_outputs('above_cutoff_lengths', 'marked strings',
                         main='below_cutoff_strings'))
below = results.below_cutoff_strings
above = results.above_cutoff_lengths
marked = results['marked strings']  # indexing works as well

# The result is also iterable, ordered in the same order that the tags were passed to with_outputs(),
# the main tag (if specified) first.

below, above, marked = (words
                        | beam.ParDo(
                            ProcessWords(), cutoff_length=2, marker='x')
                        .with_outputs('above_cutoff_lengths',
                                      'marked strings',
                                      main='below_cutoff_strings'))

```
#### 4.5.2. 在您的DoFn中发送多个输出 {#multiple-outputs-dofn}

```java
// Inside your ParDo's DoFn, you can emit an element to a specific output PCollection by providing a
// MultiOutputReceiver to your process method, and passing in the appropriate TupleTag to obtain an OutputReceiver.
// After your ParDo, extract the resulting output PCollections from the returned PCollectionTuple.
// Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.

  .of(new DoFn<String, String>() {
     public void processElement(@Element String word, MultiOutputReceiver out) {
       if (word.length() <= wordLengthCutOff) {
         // Emit short word to the main output.
         // In this example, it is the output with tag wordsBelowCutOffTag.
         out.get(wordsBelowCutOffTag).output(word);
       } else {
         // Emit long word length to the output with tag wordLengthsAboveCutOffTag.
         out.get(wordLengthsAboveCutOffTag).output(word.length());
       }
       if (word.startsWith("MARKER")) {
         // Emit word to the output with tag markedWordsTag.
         out.get(markedWordsTag).output(word);
       }
     }}));
```

```py
# Inside your ParDo's DoFn, you can emit an element to a specific output by wrapping the value and the output tag (str).
# using the pvalue.OutputValue wrapper class.
# Based on the previous example, this shows the DoFn emitting to the main output and two additional outputs.

class ProcessWords(beam.DoFn):

  def process(self, element, cutoff_length, marker):
    if len(element) <= cutoff_length:
      # Emit this short word to the main output.
      yield element
    else:
      # Emit this word's long length to the 'above_cutoff_lengths' output.
      yield pvalue.TaggedOutput(
          'above_cutoff_lengths', len(element))
    if element.startswith(marker):
      # Emit this word to a different output with the 'marked strings' tag.
      yield pvalue.TaggedOutput('marked strings', element)

# Producing multiple outputs is also available in Map and FlatMap.
# Here is an example that uses FlatMap and shows that the tags do not need to be specified ahead of time.

def even_odd(x):
  yield pvalue.TaggedOutput('odd' if x % 2 else 'even', x)
  if x % 10 == 0:
    yield x

results = numbers | beam.FlatMap(even_odd).with_outputs()

evens = results.even
odds = results.odd
tens = results[None]  # the undeclared main output

```


#### 4.5.3. 在您DoFn中访问其他的参数 {#other-dofn-parameters}

In addition to the element and the `OutputReceiver`, Beam will populate other parameters to your DoFn's `@ProcessElement` method.
Any combination of these parameters can be added to your process method in any order.


**时间戳:**
To access the timestamp of an input element, add a parameter annotated with `@Timestamp` of type `Instant`. For example:

```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, @Timestamp Instant timestamp) {
  }})
```



**窗口:**
To access the window an input element falls into, add a parameter of the type of the window used for the input `PCollection`.
If the parameter is a window type (a subclass of `BoundedWindow`) that does not match the input `PCollection`, then an error
will be raised. If an element falls in multiple windows (for example, this will happen when using `SlidingWindows`), then the
`@ProcessElement` method will be invoked multiple time for the element, once for each window. For example, when fixed windows
are being used, the window is of type `IntervalWindow`.

```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, IntervalWindow window) {
  }})
```

**PaneInfo:**
When triggers are used, Beam provides a `PaneInfo` object that contains information about the current firing. Using `PaneInfo`
you can determine whether this is an early or a late firing, and how many times this window has already fired for this key.

```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PaneInfo paneInfo) {
  }})
```

**PipelineOptions:**
The `PipelineOptions` for the current pipeline can always be accessed in a process method by adding it as a parameter:
```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PipelineOptions options) {
  }})
```

`@OnTimer` methods can also access many of these parameters. Timestamp, window, `PipelineOptions`, `OutputReceiver`, and
`MultiOutputReceiver` parameters can all be accessed in an `@OnTimer` method. In addition, an `@OnTimer` method can take
a parameter of type `TimeDomain` which tells whether the timer is based on event time or processing time.
Timers are explained in more detail in the
[Timely (and Stateful) Processing with Apache Beam]({{ site.baseurl }}/blog/2017/08/28/timely-processing.html) blog post.

### 4.6. 组合变换 {#composite-transforms}

Transforms can have a nested structure, where a complex transform performs
multiple simpler transforms (such as more than one `ParDo`, `Combine`,
`GroupByKey`, or even other composite transforms). These transforms are called
composite transforms. Nesting multiple transforms inside a single composite
transform can make your code more modular and easier to understand.

The Beam SDK comes packed with many useful composite transforms. See the API
reference pages for a list of transforms:
  * [Pre-written Beam transforms for Java](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/transforms/package-summary.html)
  * [Pre-written Beam transforms for Python](https://beam.apache.org/releases/pydoc/{{ site.release_latest }}/apache_beam.transforms.html)

#### 4.6.1. 一个组合变换的例子 {#composite-transform-example}

The `CountWords` transform in the [WordCount example program]({{ site.baseurl }}/get-started/wordcount-example/)
is an example of a composite transform. `CountWords` is a `PTransform` subclass
that consists of multiple nested transforms.

In its `expand` method, the `CountWords` transform applies the following
transform operations:

  1. It applies a `ParDo` on the input `PCollection` of text lines, producing
     an output `PCollection` of individual words.
  2. It applies the Beam SDK library transform `Count` on the `PCollection` of
     words, producing a `PCollection` of key/value pairs. Each key represents a
     word in the text, and each value represents the number of times that word
     appeared in the original data.

Note that this is also an example of nested composite transforms, as `Count`
is, by itself, a composite transform.

Your composite transform's parameters and return value must match the initial
input type and final return type for the entire transform, even if the
transform's intermediate data changes type multiple times.

```java
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // Convert lines of text into individual words.
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // Count the number of times each word occurs.
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }
```

```py
# The CountWords Composite Transform inside the WordCount pipeline.
class CountWords(beam.PTransform):

  def expand(self, pcoll):
    return (pcoll
            # Convert lines of text into individual words.
            | 'ExtractWords' >> beam.ParDo(ExtractWordsFn())
            # Count the number of times each word occurs.
            | beam.combiners.Count.PerElement()
            # Format each word and count into a printable string.
            | 'FormatCounts' >> beam.ParDo(FormatCountsFn()))

```

#### 4.6.2. 创建一个组合变换 {#composite-transform-creation}

To create your own composite transform, create a subclass of the `PTransform`
class and override the `expand` method to specify the actual processing logic.
You can then use this transform just as you would a built-in transform from the
Beam SDK.

{:.language-java}
For the `PTransform` class type parameters, you pass the `PCollection` types
that your transform takes as input, and produces as output. To take multiple
`PCollection`s as input, or produce multiple `PCollection`s as output, use one
of the multi-collection types for the relevant type parameter.

The following code sample shows how to declare a `PTransform` that accepts a
`PCollection` of `String`s for input, and outputs a `PCollection` of `Integer`s:

```java
  static class ComputeWordLengths
    extends PTransform<PCollection<String>, PCollection<Integer>> {
    ...
  }
```

```Py
class ComputeWordLengths(beam.PTransform):
  def expand(self, pcoll):
    # Transform logic goes here.
    return pcoll | beam.Map(lambda x: len(x))
```

Within your `PTransform` subclass, you'll need to override the `expand` method.
The `expand` method is where you add the processing logic for the `PTransform`.
Your override of `expand` must accept the appropriate type of input
`PCollection` as a parameter, and specify the output `PCollection` as the return
value.

The following code sample shows how to override `expand` for the
`ComputeWordLengths` class declared in the previous example:

```java
  static class ComputeWordLengths
      extends PTransform<PCollection<String>, PCollection<Integer>> {
    @Override
    public PCollection<Integer> expand(PCollection<String>) {
      ...
      // transform logic goes here
      ...
    }
```

```py
class ComputeWordLengths(beam.PTransform):
  def expand(self, pcoll):
    # Transform logic goes here.
    return pcoll | beam.Map(lambda x: len(x))
```

As long as you override the `expand` method in your `PTransform` subclass to
accept the appropriate input `PCollection`(s) and return the corresponding
output `PCollection`(s), you can include as many transforms as you want. These
transforms can include core transforms, composite transforms, or the transforms
included in the Beam SDK libraries.

**Note:** The `expand` method of a `PTransform` is not meant to be invoked
directly by the user of a transform. Instead, you should call the `apply` method
on the `PCollection` itself, with the transform as an argument. This allows
transforms to be nested within the structure of your pipeline.

#### 4.6.3. PTransform风格指南 {#ptransform-style-guide}

The [PTransform Style Guide]({{ site.baseurl }}/contribute/ptransform-style-guide/)
contains additional information not included here, such as style guidelines,
logging and testing guidance, and language-specific considerations.  The guide
is a useful starting point when you want to write new composite PTransforms.

## 5. 管道I/O. {#pipeline-io}

When you create a pipeline, you often need to read data from some external
source, such as a file or a database. Likewise, you may
want your pipeline to output its result data to an external storage system.
Beam provides read and write transforms for a [number of common data storage
types]({{ site.baseurl }}/documentation/io/built-in/). If you want your pipeline
to read from or write to a data storage format that isn't supported by the
built-in transforms, you can [implement your own read and write
transforms]({{site.baseurl }}/documentation/io/developing-io-overview/).

### 5.1. 读取输入数据 {#pipeline-io-reading-data}

Read transforms read data from an external source and return a `PCollection`
representation of the data for use by your pipeline. You can use a read
transform at any point while constructing your pipeline to create a new
`PCollection`, though it will be most common at the start of your pipeline.

```java
PCollection<String> lines = p.apply(TextIO.read().from("gs://some/inputData.txt"));
```

```py
lines = pipeline | beam.io.ReadFromText('gs://some/inputData.txt')
```

### 5.2. 写入输出数据 {#pipeline-io-writing-data}

Write transforms write the data in a `PCollection` to an external data source.
You will most often use write transforms at the end of your pipeline to output
your pipeline's final results. However, you can use a write transform to output
a `PCollection`'s data at any point in your pipeline.

```java
output.apply(TextIO.write().to("gs://some/outputData"));
```

```py
output | beam.io.WriteToText('gs://some/outputData')
```

### 5.3. 基于文件的输入和输出数据 {#file-based-data}

#### 5.3.1. 从多个位置读取 {#file-based-reading-multiple-locations}

Many read transforms support reading from multiple input files matching a glob
operator you provide. Note that glob operators are filesystem-specific and obey
filesystem-specific consistency models. The following TextIO example uses a glob
operator (\*) to read all matching input files that have prefix "input-" and the
suffix ".csv" in the given location:

```java
p.apply(“ReadFromText”,
    TextIO.read().from("protocol://my_bucket/path/to/input-*.csv");
```

```py
lines = p | 'ReadFromText' >> beam.io.ReadFromText('path/to/input-*.csv')
```

To read data from disparate sources into a single `PCollection`, read each one
independently and then use the [Flatten](#flatten) transform to create a single
`PCollection`.

#### 5.3.2. 写入多个输出文件 {#file-based-writing-multiple-files}

For file-based output data, write transforms write to multiple output files by
default. When you pass an output file name to a write transform, the file name
is used as the prefix for all output files that the write transform produces.
You can append a suffix to each output file by specifying a suffix.

The following write transform example writes multiple output files to a
location. Each file has the prefix "numbers", a numeric tag, and the suffix
".csv".

```java
records.apply("WriteToText",
    TextIO.write().to("protocol://my_bucket/path/to/numbers")
                .withSuffix(".csv"));
```

```py
filtered_words | 'WriteToText' >> beam.io.WriteToText(
    '/path/to/numbers', file_name_suffix='.csv')
```

### 5.4. Beam提供的I/O变换 {#provided-io-transforms}

See the [Beam-provided I/O Transforms]({{site.baseurl }}/documentation/io/built-in/)
page for a list of the currently available I/O transforms.

## 6. 数据编码和类型安全 {#data-encoding-and-type-safety}

When Beam runners execute your pipeline, they often need to materialize the
intermediate data in your `PCollection`s, which requires converting elements to
and from byte strings. The Beam SDKs use objects called `Coder`s to describe how
the elements of a given `PCollection` may be encoded and decoded.

> Note that coders are unrelated to parsing or formatting data when interacting
> with external data sources or sinks. Such parsing or formatting should
> typically be done explicitly, using transforms such as `ParDo` or
> `MapElements`.


In the Beam SDK for Java, the type `Coder` provides the methods required for
encoding and decoding data. The SDK for Java provides a number of Coder
subclasses that work with a variety of standard Java types, such as Integer,
Long, Double, StringUtf8 and more. You can find all of the available Coder
subclasses in the [Coder package](https://github.com/apache/beam/tree/master/sdks/java/core/src/main/java/org/apache/beam/sdk/coders).


In the Beam SDK for Python, the type `Coder` provides the methods required for
encoding and decoding data. The SDK for Python provides a number of Coder
subclasses that work with a variety of standard Python types, such as primitive
types, Tuple, Iterable, StringUtf8 and more. You can find all of the available
Coder subclasses in the
[apache_beam.coders](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/coders)
package.

> Note that coders do not necessarily have a 1:1 relationship with types. For
> example, the Integer type can have multiple valid coders, and input and output
> data can use different Integer coders. A transform might have Integer-typed
> input data that uses BigEndianIntegerCoder, and Integer-typed output data that
> uses VarIntCoder.

### 6.1. 指定编码器 {#specifying-coders}

The Beam SDKs require a coder for every `PCollection` in your pipeline. In most
cases, the Beam SDK is able to automatically infer a `Coder` for a `PCollection`
based on its element type or the transform that produces it, however, in some
cases the pipeline author will need to specify a `Coder` explicitly, or develop
a `Coder` for their custom type.


You can explicitly set the coder for an existing `PCollection` by using the
method `PCollection.setCoder`. Note that you cannot call `setCoder` on a
`PCollection` that has been finalized (e.g. by calling `.apply` on it).


You can get the coder for an existing `PCollection` by using the method
`getCoder`. This method will fail with an `IllegalStateException` if a coder has
not been set and cannot be inferred for the given `PCollection`.

Beam SDKs use a variety of mechanisms when attempting to automatically infer the
`Coder` for a `PCollection`.


Each pipeline object has a `CoderRegistry`. The `CoderRegistry` represents a
mapping of Java types to the default coders that the pipeline should use for
`PCollection`s of each type.


The Beam SDK for Python has a `CoderRegistry` that represents a mapping of
Python types to the default coder that should be used for `PCollection`s of each
type.


By default, the Beam SDK for Java automatically infers the `Coder` for the
elements of a `PCollection` produced by a `PTransform` using the type parameter
from the transform's function object, such as `DoFn`. In the case of `ParDo`,
for example, a `DoFn<Integer, String>` function object accepts an input element
of type `Integer` and produces an output element of type `String`. In such a
case, the SDK for Java will automatically infer the default `Coder` for the
output `PCollection<String>` (in the default pipeline `CoderRegistry`, this is
`StringUtf8Coder`).


By default, the Beam SDK for Python automatically infers the `Coder` for the
elements of an output `PCollection` using the typehints from the transform's
function object, such as `DoFn`. In the case of `ParDo`, for example a `DoFn`
with the typehints `@beam.typehints.with_input_types(int)` and
`@beam.typehints.with_output_types(str)` accepts an input element of type int
and produces an output element of type str. In such a case, the Beam SDK for
Python will automatically infer the default `Coder` for the output `PCollection`
(in the default pipeline `CoderRegistry`, this is `BytesCoder`).

> NOTE: If you create your `PCollection` from in-memory data by using the
> `Create` transform, you cannot rely on coder inference and default coders.
> `Create` does not have access to any typing information for its arguments, and
> may not be able to infer a coder if the argument list contains a value whose
> exact run-time class doesn't have a default coder registered.


When using `Create`, the simplest way to ensure that you have the correct coder
is by invoking `withCoder` when you apply the `Create` transform.

### 6.2. 默认编码器和CoderRegistry   {#default-coders-and-the-coderregistry}

Each Pipeline object has a `CoderRegistry` object, which maps language types to
the default coder the pipeline should use for those types. You can use the
`CoderRegistry` yourself to look up the default coder for a given type, or to
register a new default coder for a given type.

`CoderRegistry` contains a default mapping of coders to standard
<span class="language-java">Java</span><span class="language-py">Python</span>
types for any pipeline you create using the Beam SDK for
<span class="language-java">Java</span><span class="language-py">Python</span>.
The following table shows the standard mapping:

<table>
  <thead>
    <tr class="header">
      <th>Java Type</th>
      <th>Default Coder</th>
    </tr>
  </thead>
  <tbody>
    <tr class="odd">
      <td>Double</td>
      <td>DoubleCoder</td>
    </tr>
    <tr class="even">
      <td>Instant</td>
      <td>InstantCoder</td>
    </tr>
    <tr class="odd">
      <td>Integer</td>
      <td>VarIntCoder</td>
    </tr>
    <tr class="even">
      <td>Iterable</td>
      <td>IterableCoder</td>
    </tr>
    <tr class="odd">
      <td>KV</td>
      <td>KvCoder</td>
    </tr>
    <tr class="even">
      <td>List</td>
      <td>ListCoder</td>
    </tr>
    <tr class="odd">
      <td>Map</td>
      <td>MapCoder</td>
    </tr>
    <tr class="even">
      <td>Long</td>
      <td>VarLongCoder</td>
    </tr>
    <tr class="odd">
      <td>String</td>
      <td>StringUtf8Coder</td>
    </tr>
    <tr class="even">
      <td>TableRow</td>
      <td>TableRowJsonCoder</td>
    </tr>
    <tr class="odd">
      <td>Void</td>
      <td>VoidCoder</td>
    </tr>
    <tr class="even">
      <td>byte[ ]</td>
      <td>ByteArrayCoder</td>
    </tr>
    <tr class="odd">
      <td>TimestampedValue</td>
      <td>TimestampedValueCoder</td>
    </tr>
  </tbody>
</table>

<table>
  <thead>
    <tr class="header">
      <th>Python Type</th>
      <th>Default Coder</th>
    </tr>
  </thead>
  <tbody>
    <tr class="odd">
      <td>int</td>
      <td>VarIntCoder</td>
    </tr>
    <tr class="even">
      <td>float</td>
      <td>FloatCoder</td>
    </tr>
    <tr class="odd">
      <td>str</td>
      <td>BytesCoder</td>
    </tr>
    <tr class="even">
      <td>bytes</td>
      <td>StrUtf8Coder</td>
    </tr>
    <tr class="odd">
      <td>Tuple</td>
      <td>TupleCoder</td>
    </tr>
  </tbody>
</table>

#### 6.2.1. 查找一个默认编码器 {#default-coder-lookup}


You can use the method `CoderRegistry.getCoder` to determine the default
Coder for a Java type. You can access the `CoderRegistry` for a given pipeline
by using the method `Pipeline.getCoderRegistry`. This allows you to determine
(or set) the default Coder for a Java type on a per-pipeline basis: i.e. "for
this pipeline, verify that Integer values are encoded using
`BigEndianIntegerCoder`."


You can use the method `CoderRegistry.get_coder` to determine the default Coder
for a Python type. You can use `coders.registry` to access the `CoderRegistry`.
This allows you to determine (or set) the default Coder for a Python type.

#### 6.2.2. 设置一个类型的默认编码器 {#setting-default-coder}

To set the default Coder for a
<span class="language-java">Java</span><span class="language-py">Python</span>
type for a particular pipeline, you obtain and modify the pipeline's
`CoderRegistry`. You use the method
<span class="language-java">`Pipeline.getCoderRegistry`</span>
<span class="language-py">`coders.registry`</span>
to get the `CoderRegistry` object, and then use the method
<span class="language-java">`CoderRegistry.registerCoder`</span>
<span class="language-py">`CoderRegistry.register_coder`</span>
to register a new `Coder` for the target type.

The following example code demonstrates how to set a default Coder, in this case
`BigEndianIntegerCoder`, for
<span class="language-java">Integer</span><span class="language-py">int</span>
values for a pipeline.

```java
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);

CoderRegistry cr = p.getCoderRegistry();
cr.registerCoder(Integer.class, BigEndianIntegerCoder.class);
```

```py
apache_beam.coders.registry.register_coder(int, BigEndianIntegerCoder)
```

#### 6.2.3. 使用默认编码器注解自定义数据类型 {#annotating-custom-type-default-coder}


If your pipeline program defines a custom data type, you can use the
`@DefaultCoder` annotation to specify the coder to use with that type. For
example, let's say you have a custom data type for which you want to use
`SerializableCoder`. You can use the `@DefaultCoder` annotation as follows:

```java
@DefaultCoder(AvroCoder.class)
public class MyCustomDataType {
  ...
}
```


If you've created a custom coder to match your data type, and you want to use
the `@DefaultCoder` annotation, your coder class must implement a static
`Coder.of(Class<T>)` factory method.

```java
public class MyCustomCoder implements Coder {
  public static Coder<T> of(Class<T> clazz) {...}
  ...
}

@DefaultCoder(MyCustomCoder.class)
public class MyCustomDataType {
  ...
}
```


The Beam SDK for Python does not support annotating data types with a default
coder. If you would like to set a default coder, use the method described in the
previous section, *Setting the default coder for a type*.

## 7. 窗口 {#windowing}

Windowing subdivides a `PCollection` according to the timestamps of its
individual elements. Transforms that aggregate multiple elements, such as
`GroupByKey` and `Combine`, work implicitly on a per-window basis — they process
each `PCollection` as a succession of multiple, finite windows, though the
entire collection itself may be of unbounded size.

A related concept, called **triggers**, determines when to emit the results of
aggregation as unbounded data arrives. You can use triggers to refine the
windowing strategy for your `PCollection`. Triggers allow you to deal with
late-arriving data or to provide early results. See the [triggers](#triggers)
section for more information.

### 7.1.窗口基础知识 {#windowing-basics}

Some Beam transforms, such as `GroupByKey` and `Combine`, group multiple
elements by a common key. Ordinarily, that grouping operation groups all of the
elements that have the same key within the entire data set. With an unbounded
data set, it is impossible to collect all of the elements, since new elements
are constantly being added and may be infinitely many (e.g. streaming data). If
you are working with unbounded `PCollection`s, windowing is especially useful.

In the Beam model, any `PCollection` (including unbounded `PCollection`s) can be
subdivided into logical windows. Each element in a `PCollection` is assigned to
one or more windows according to the `PCollection`'s windowing function, and
each individual window contains a finite number of elements. Grouping transforms
then consider each `PCollection`'s elements on a per-window basis. `GroupByKey`,
for example, implicitly groups the elements of a `PCollection` by _key and
window_.

**Caution:** Beam's default windowing behavior is to assign all elements of a
`PCollection` to a single, global window and discard late data, _even for
unbounded `PCollection`s_. Before you use a grouping transform such as
`GroupByKey` on an unbounded `PCollection`, you must do at least one of the
following:
 * Set a non-global windowing function. See [Setting your PCollection's
   windowing function](#setting-your-pcollections-windowing-function).
 * Set a non-default [trigger](#triggers). This allows the global window to emit
   results under other conditions, since the default windowing behavior (waiting
   for all data to arrive) will never occur.

If you don't set a non-global windowing function or a non-default trigger for
your unbounded `PCollection` and subsequently use a grouping transform such as
`GroupByKey` or `Combine`, your pipeline will generate an error upon
construction and your job will fail.

#### 7.1.1. 窗口约束 {#windowing-constraints}

After you set the windowing function for a `PCollection`, the elements' windows
are used the next time you apply a grouping transform to that `PCollection`.
Window grouping occurs on an as-needed basis. If you set a windowing function
using the `Window` transform, each element is assigned to a window, but the
windows are not considered until `GroupByKey` or `Combine` aggregates across a
window and key. This can have different effects on your pipeline.  Consider the
example pipeline in the figure below:

![Diagram of pipeline applying windowing]({{ "/images/windowing-pipeline-unbounded.png" | prepend: site.baseurl }} "Pipeline applying windowing")

**Figure:** Pipeline applying windowing

In the above pipeline, we create an unbounded `PCollection` by reading a set of
key/value pairs using `KafkaIO`, and then apply a windowing function to that
collection using the `Window` transform. We then apply a `ParDo` to the the
collection, and then later group the result of that `ParDo` using `GroupByKey`.
The windowing function has no effect on the `ParDo` transform, because the
windows are not actually used until they're needed for the `GroupByKey`.
Subsequent transforms, however, are applied to the result of the `GroupByKey` --
data is grouped by both key and window.

#### 7.1.2. 使用有界PCollections进行窗口化 {#windowing-bounded-collections}

You can use windowing with fixed-size data sets in **bounded** `PCollection`s.
However, note that windowing considers only the implicit timestamps attached to
each element of a `PCollection`, and data sources that create fixed data sets
(such as `TextIO`) assign the same timestamp to every element. This means that
all the elements are by default part of a single, global window.

To use windowing with fixed data sets, you can assign your own timestamps to
each element. To assign timestamps to elements, use a `ParDo` transform with a
`DoFn` that outputs each element with a new timestamp (for example, the
[WithTimestamps](https://beam.apache.org/releases/javadoc/{{ site.release_latest }}/index.html?org/apache/beam/sdk/transforms/WithTimestamps.html)
transform in the Beam SDK for Java).

To illustrate how windowing with a bounded `PCollection` can affect how your
pipeline processes data, consider the following pipeline:

![Diagram of GroupByKey and ParDo without windowing, on a bounded collection]({{ "/images/unwindowed-pipeline-bounded.png" | prepend: site.baseurl }} "GroupByKey and ParDo without windowing, on a bounded collection")

**Figure:** `GroupByKey` and `ParDo` without windowing, on a bounded collection.

In the above pipeline, we create a bounded `PCollection` by reading a set of
key/value pairs using `TextIO`. We then group the collection using `GroupByKey`,
and apply a `ParDo` transform to the grouped `PCollection`. In this example, the
`GroupByKey` creates a collection of unique keys, and then `ParDo` gets applied
exactly once per key.

Note that even if you don’t set a windowing function, there is still a window --
all elements in your `PCollection` are assigned to a single global window.

Now, consider the same pipeline, but using a windowing function:

![Diagram of GroupByKey and ParDo with windowing, on a bounded collection]({{ "/images/windowing-pipeline-bounded.png" | prepend: site.baseurl }} "GroupByKey and ParDo with windowing, on a bounded collection")

**Figure:** `GroupByKey` and `ParDo` with windowing, on a bounded collection.

As before, the pipeline creates a bounded `PCollection` of key/value pairs. We
then set a [windowing function](#setting-your-pcollections-windowing-function)
for that `PCollection`.  The `GroupByKey` transform groups the elements of the
`PCollection` by both key and window, based on the windowing function. The
subsequent `ParDo` transform gets applied multiple times per key, once for each
window.

### 7.2. 提供了窗口函数 {#provided-windowing-functions}

You can define different kinds of windows to divide the elements of your
`PCollection`. Beam provides several windowing functions, including:

*  Fixed Time Windows
*  Sliding Time Windows
*  Per-Session Windows
*  Single Global Window
*  Calendar-based Windows (not supported by the Beam SDK for Python)

You can also define your own `WindowFn` if you have a more complex need.

Note that each element can logically belong to more than one window, depending
on the windowing function you use. Sliding time windowing, for example, creates
overlapping windows wherein a single element can be assigned to multiple
windows.


#### 7.2.1. 固定时间窗口 {#fixed-time-windows}

The simplest form of windowing is using **fixed time windows**: given a
timestamped `PCollection` which might be continuously updating, each window
might capture (for example) all elements with timestamps that fall into a five
minute interval.

A fixed time window represents a consistent duration, non overlapping time
interval in the data stream. Consider windows with a five-minute duration: all
of the elements in your unbounded `PCollection` with timestamp values from
0:00:00 up to (but not including) 0:05:00 belong to the first window, elements
with timestamp values from 0:05:00 up to (but not including) 0:10:00 belong to
the second window, and so on.

![Diagram of fixed time windows, 30s in duration]({{ "/images/fixed-time-windows.png" | prepend: site.baseurl }} "Fixed time windows, 30s in duration")

**Figure:** Fixed time windows, 30s in duration.

#### 7.2.2. 滑动时间窗口 {#sliding-time-windows}

A **sliding time window** also represents time intervals in the data stream;
however, sliding time windows can overlap. For example, each window might
capture five minutes worth of data, but a new window starts every ten seconds.
The frequency with which sliding windows begin is called the _period_.
Therefore, our example would have a window _duration_ of five minutes and a
_period_ of ten seconds.

Because multiple windows overlap, most elements in a data set will belong to
more than one window. This kind of windowing is useful for taking running
averages of data; using sliding time windows, you can compute a running average
of the past five minutes' worth of data, updated every ten seconds, in our
example.

![Diagram of sliding time windows, with 1 minute window duration and 30s window period]({{ "/images/sliding-time-windows.png" | prepend: site.baseurl }} "Sliding time windows, with 1 minute window duration and 30s window period")

**Figure:** Sliding time windows, with 1 minute window duration and 30s window
period.

#### 7.2.3. 会话窗口 {#session-windows}

A **session window** function defines windows that contain elements that are
within a certain gap duration of another element. Session windowing applies on a
per-key basis and is useful for data that is irregularly distributed with
respect to time. For example, a data stream representing user mouse activity may
have long periods of idle time interspersed with high concentrations of clicks.
If data arrives after the minimum specified gap duration time, this initiates
the start of a new window.

![Diagram of session windows with a minimum gap duration]({{ "/images/session-windows.png" | prepend: site.baseurl }} "Session windows, with a minimum gap duration")

**Figure:** Session windows, with a minimum gap duration. Note how each data key
has different windows, according to its data distribution.

#### 7.2.4. 单一的全局窗口 {#single-global-window}

By default, all data in a `PCollection` is assigned to the single global window,
and late data is discarded. If your data set is of a fixed size, you can use the
global window default for your `PCollection`.

You can use the single global window if you are working with an unbounded data set
(e.g. from a streaming data source) but use caution when applying aggregating
transforms such as `GroupByKey` and `Combine`. The single global window with a
default trigger generally requires the entire data set to be available before
processing, which is not possible with continuously updating data. To perform
aggregations on an unbounded `PCollection` that uses global windowing, you
should specify a non-default trigger for that `PCollection`.

### 7.3. 设置您PCollection的窗口函数 {#setting-your-pcollections-windowing-function}

You can set the windowing function for a `PCollection` by applying the `Window`
transform. When you apply the `Window` transform, you must provide a `WindowFn`.
The `WindowFn` determines the windowing function your `PCollection` will use for
subsequent grouping transforms, such as a fixed or sliding time window.

When you set a windowing function, you may also want to set a trigger for your
`PCollection`. The trigger determines when each individual window is aggregated
and emitted, and helps refine how the windowing function performs with respect
to late data and computing early results. See the [triggers](#triggers) section
for more information.

#### 7.3.1. 固定时间窗口 {#using-fixed-time-windows}

The following example code shows how to apply `Window` to divide a `PCollection`
into fixed windows, each 60 seconds in length:

```java
    PCollection<String> items = ...;
    PCollection<String> fixedWindowedItems = items.apply(
        Window.<String>into(FixedWindows.of(Duration.standardSeconds(60))));
```
```py
from apache_beam import window
fixed_windowed_items = (
    items | 'window' >> beam.WindowInto(window.FixedWindows(60)))
```

#### 7.3.2. 滑动时间窗口 {#using-sliding-time-windows}

The following example code shows how to apply `Window` to divide a `PCollection`
into sliding time windows. Each window is 30 seconds in length, and a new window
begins every five seconds:

```java
    PCollection<String> items = ...;
    PCollection<String> slidingWindowedItems = items.apply(
        Window.<String>into(SlidingWindows.of(Duration.standardSeconds(30)).every(Duration.standardSeconds(5))));
```
```py
from apache_beam import window
sliding_windowed_items = (
    items | 'window' >> beam.WindowInto(window.SlidingWindows(30, 5)))
```

#### 7.3.3. 会话窗口 {#using-session-windows}

The following example code shows how to apply `Window` to divide a `PCollection`
into session windows, where each session must be separated by a time gap of at
least 10 minutes (600 seconds):

```java
    PCollection<String> items = ...;
    PCollection<String> sessionWindowedItems = items.apply(
        Window.<String>into(Sessions.withGapDuration(Duration.standardSeconds(600))));
```
```py
from apache_beam import window
session_windowed_items = (
    items | 'window' >> beam.WindowInto(window.Sessions(10 * 60)))
```

Note that the sessions are per-key — each key in the collection will have its
own session groupings depending on the data distribution.

#### 7.3.4. 单一的全局窗口 {#using-single-global-window}

If your `PCollection` is bounded (the size is fixed), you can assign all the
elements to a single global window. The following example code shows how to set
a single global window for a `PCollection`:

```java
    PCollection<String> items = ...;
    PCollection<String> batchItems = items.apply(
        Window.<String>into(new GlobalWindows()));
```
```py
from apache_beam import window
session_windowed_items = (
    items | 'window' >> beam.WindowInto(window.GlobalWindows()))
```

### 7.4. 水印和后期数据 {#watermarks-and-late-data}

In any data processing system, there is a certain amount of lag between the time
a data event occurs (the "event time", determined by the timestamp on the data
element itself) and the time the actual data element gets processed at any stage
in your pipeline (the "processing time", determined by the clock on the system
processing the element). In addition, there are no guarantees that data events
will appear in your pipeline in the same order that they were generated.

For example, let's say we have a `PCollection` that's using fixed-time
windowing, with windows that are five minutes long. For each window, Beam must
collect all the data with an _event time_ timestamp in the given window range
(between 0:00 and 4:59 in the first window, for instance). Data with timestamps
outside that range (data from 5:00 or later) belong to a different window.

However, data isn't always guaranteed to arrive in a pipeline in time order, or
to always arrive at predictable intervals. Beam tracks a _watermark_, which is
the system's notion of when all data in a certain window can be expected to have
arrived in the pipeline. Once the watermark progresses past the end of a window,
any further element that arrives with a timestamp in that window is considered
**后期数据**.

From our example, suppose we have a simple watermark that assumes approximately
30s of lag time between the data timestamps (the event time) and the time the
data appears in the pipeline (the processing time), then Beam would close the
first window at 5:30. If a data record arrives at 5:34, but with a timestamp
that would put it in the 0:00-4:59 window (say, 3:38), then that record is late
data.

Note: For simplicity, we've assumed that we're using a very straightforward
watermark that estimates the lag time. In practice, your `PCollection`'s data
source determines the watermark, and watermarks can be more precise or complex.

Beam's default windowing configuration tries to determines when all data has
arrived (based on the type of data source) and then advances the watermark past
the end of the window. This default configuration does _not_ allow late data.
[Triggers](#triggers) allow you to modify and refine the windowing strategy for
a `PCollection`. You can use triggers to decide when each individual window
aggregates and reports its results, including how the window emits late
elements.

#### 7.4.1. 管理后期数据 {#managing-late-data}

> **Note:** Managing late data is not supported in the Beam SDK for Python.

You can allow late data by invoking the `.withAllowedLateness` operation when
you set your `PCollection`'s windowing strategy. The following code example
demonstrates a windowing strategy that will allow late data up to two days after
the end of a window.

```java
    PCollection<String> items = ...;
    PCollection<String> fixedWindowedItems = items.apply(
        Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
              .withAllowedLateness(Duration.standardDays(2)));
```

When you set `.withAllowedLateness` on a `PCollection`, that allowed lateness
propagates forward to any subsequent `PCollection` derived from the first
`PCollection` you applied allowed lateness to. If you want to change the allowed
lateness later in your pipeline, you must do so explictly by applying
`Window.configure().withAllowedLateness()`.

### 7.5. 将时间戳添加到一个PCollection的元素{#adding-timestamps-to-a-pcollections-elements}

An unbounded source provides a timestamp for each element. Depending on your
unbounded source, you may need to configure how the timestamp is extracted from
the raw data stream.

However, bounded sources (such as a file from `TextIO`) do not provide
timestamps. If you need timestamps, you must add them to your `PCollection`’s
elements.

You can assign new timestamps to the elements of a `PCollection` by applying a
[ParDo](#pardo) transform that outputs new elements with timestamps that you
set.

An example might be if your pipeline reads log records from an input file, and
each log record includes a timestamp field; since your pipeline reads the
records in from a file, the file source doesn't assign timestamps automatically.
You can parse the timestamp field from each record and use a `ParDo` transform
with a `DoFn` to attach the timestamps to each element in your `PCollection`.

```java
      PCollection<LogEntry> unstampedLogs = ...;
      PCollection<LogEntry> stampedLogs =
          unstampedLogs.apply(ParDo.of(new DoFn<LogEntry, LogEntry>() {
            public void processElement(@Element LogEntry element, OutputReceiver<LogEntry> out) {
              // Extract the timestamp from log entry we're currently processing.
              Instant logTimeStamp = extractTimeStampFromLogEntry(element);
              // Use OutputReceiver.outputWithTimestamp (rather than
              // OutputReceiver.output) to emit the entry with timestamp attached.
              out.outputWithTimestamp(element, logTimeStamp);
            }
          }));
```
```py
class AddTimestampDoFn(beam.DoFn):

  def process(self, element):
    # Extract the numeric Unix seconds-since-epoch timestamp to be
    # associated with the current log entry.
    unix_timestamp = extract_timestamp_from_log_entry(element)
    # Wrap and emit the current entry and new timestamp in a
    # TimestampedValue.
    yield beam.window.TimestampedValue(element, unix_timestamp)

timestamped_items = items | 'timestamp' >> beam.ParDo(AddTimestampDoFn())

```

## 8. 触发器 {#triggers}

When collecting and grouping data into windows, Beam uses **triggers** to
determine when to emit the aggregated results of each window (referred to as a
*pane*). If you use Beam's default windowing configuration and [default
trigger](#default-trigger), Beam outputs the aggregated result when it
[estimates all data has arrived](#watermarks-and-late-data), and discards all
subsequent data for that window.

You can set triggers for your `PCollection`s to change this default behavior.
Beam provides a number of pre-built triggers that you can set:

*   **事件时间触发器**. These triggers operate on the event time, as
    indicated by the timestamp on each data element. Beam's default trigger is
    event time-based.
*   **处理时间触发器**. These triggers operate on the processing time
    -- the time when the data element is processed at any given stage in the
    pipeline.
*   **数据驱动触发器**. These triggers operate by examining the data as it
    arrives in each window, and firing when that data meets a certain property.
    Currently, data-driven triggers only support firing after a certain number
    of data elements.
*   **组合触发器**. These triggers combine multiple triggers in various
    ways.

At a high level, triggers provide two additional capabilities compared to simply
outputting at the end of a window:

*   Triggers allow Beam to emit early results, before all the data in a given
    window has arrived. For example, emitting after a certain amount of time
    elapses, or after a certain number of elements arrives.
*   Triggers allow processing of late data by triggering after the event time
    watermark passes the end of the window.

These capabilities allow you to control the flow of your data and balance
between different factors depending on your use case:

*   **完整性:** How important is it to have all of your data before you
    compute your result?
*   **延迟:** How long do you want to wait for data? For example, do you wait
    until you think you have all data? Do you process data as it arrives?
*   **成本:** How much compute power/money are you willing to spend to lower the
    latency?

For example, a system that requires time-sensitive updates might use a strict
time-based trigger that emits a window every *N* seconds, valuing promptness
over data completeness. A system that values data completeness more than the
exact timing of results might choose to use Beam's default trigger, which fires
at the end of the window.

You can also set a trigger for an unbounded `PCollection` that uses a [single
global window for its windowing function](#windowing). This can be useful when
you want your pipeline to provide periodic updates on an unbounded data set —
for example, a running average of all data provided to the present time, updated
every N seconds or every N elements.

### 8.1. 事件时间触发器 {#event-time-triggers}

The `AfterWatermark` trigger operates on *event time*. The `AfterWatermark`
trigger emits the contents of a window after the
[watermark](#watermarks-and-late-data) passes the end of the window, based on the
timestamps attached to the data elements. The watermark is a global progress
metric, and is Beam's notion of input completeness within your pipeline at any
given point. <span class="language-java">`AfterWatermark.pastEndOfWindow()`</span>
<span class="language-py">`AfterWatermark`</span> *only* fires when the
watermark passes the end of the window.

In addition, you can configure triggers that fire if your pipeline receives data
before or after the end of the window.

The following example shows a billing scenario, and uses both early and late
firings:

```java
  // Create a bill at the end of the month.
  AfterWatermark.pastEndOfWindow()
      // During the month, get near real-time estimates.
      .withEarlyFirings(
          AfterProcessingTime
              .pastFirstElementInPane()
              .plusDuration(Duration.standardMinutes(1))
      // Fire on any late data so the bill can be corrected.
      .withLateFirings(AfterPane.elementCountAtLeast(1))
```
```py
AfterWatermark(
    early=AfterProcessingTime(delay=1 * 60),
    late=AfterCount(1))

```

#### 8.1.1. 默认触发器 {#default-trigger}

The default trigger for a `PCollection` is based on event time, and emits the
results of the window when the Beam's watermark passes the end of the window,
and then fires each time late data arrives.

However, if you are using both the default windowing configuration and the
default trigger, the default trigger emits exactly once, and late data is
discarded. This is because the default windowing configuration has an allowed
lateness value of 0. See the Handling Late Data section for information about
modifying this behavior.

### 8.2. 处理时间触发器 {#processing-time-triggers}

The `AfterProcessingTime` trigger operates on *processing time*. For example,
the <span class="language-java">`AfterProcessingTime.pastFirstElementInPane()`</span>
<span class="language-py">`AfterProcessingTime`</span> trigger emits a window
after a certain amount of processing time has passed since data was received.
The processing time is determined by the system clock, rather than the data
element's timestamp.

The `AfterProcessingTime` trigger is useful for triggering early results from a
window, particularly a window with a large time frame such as a single global
window.

### 8.3. 数据驱动的触发器 {#data-driven-triggers}

Beam provides one data-driven trigger,
<span class="language-java">`AfterPane.elementCountAtLeast()`</span>
<span class="language-py">`AfterCount`</span>. This trigger works on an element
count; it fires after the current pane has collected at least *N* elements. This
allows a window to emit early results (before all the data has accumulated),
which can be particularly useful if you are using a single global window.

It is important to note that if, for example, you specify
<span class="language-java">`.elementCountAtLeast(50)`</span>
<span class="language-py">AfterCount(50)</span> and only 32 elements arrive,
those 32 elements sit around forever. If the 32 elements are important to you,
consider using [composite triggers](#composite-triggers) to combine multiple
conditions. This allows you to specify multiple firing conditions such as “fire
either when I receive 50 elements, or every 1 second”.

### 8.4. 设置一个触发器 {#setting-a-trigger}

When you set a windowing function for a `PCollection` by using the
<span class="language-java">`Window`</span><span class="language-py">`WindowInto`</span>
transform, you can also specify a trigger.

You set the trigger(s) for a `PCollection` by invoking the method
`.triggering()` on the result of your `Window.into()` transform. This code
sample sets a time-based trigger for a `PCollection`, which emits results one
minute after the first element in that window has been processed.  The last line
in the code sample, `.discardingFiredPanes()`, sets the window's **accumulation
mode**.

You set the trigger(s) for a `PCollection` by setting the `trigger` parameter
when you use the `WindowInto` transform. This code sample sets a time-based
trigger for a `PCollection`, which emits results one minute after the first
element in that window has been processed. The `accumulation_mode` parameter
sets the window's **accumulation mode**.

```java
  PCollection<String> pc = ...;
  pc.apply(Window.<String>into(FixedWindows.of(1, TimeUnit.MINUTES))
                               .triggering(AfterProcessingTime.pastFirstElementInPane()
                                                              .plusDelayOf(Duration.standardMinutes(1)))
                               .discardingFiredPanes());
```
```py
pcollection | WindowInto(
    FixedWindows(1 * 60),
    trigger=AfterProcessingTime(10 * 60),
    accumulation_mode=AccumulationMode.DISCARDING)
```

#### 8.4.1. 窗口累积模式 {#window-accumulation-modes}

When you specify a trigger, you must also set the the window's **accumulation
mode**. When a trigger fires, it emits the current contents of the window as a
pane. Since a trigger can fire multiple times, the accumulation mode determines
whether the system *accumulates* the window panes as the trigger fires, or
*discards* them.

To set a window to accumulate the panes that are produced when the trigger
fires, invoke`.accumulatingFiredPanes()` when you set the trigger. To set a
window to discard fired panes, invoke `.discardingFiredPanes()`.

To set a window to accumulate the panes that are produced when the trigger
fires, set the `accumulation_mode` parameter to `ACCUMULATING` when you set the
trigger. To set a window to discard fired panes, set `accumulation_mode` to
`DISCARDING`.

Let's look an example that uses a `PCollection` with fixed-time windowing and a
data-based trigger. This is something you might do if, for example, each window
represented a ten-minute running average, but you wanted to display the current
value of the average in a UI more frequently than every ten minutes. We'll
assume the following conditions:

*   The `PCollection` uses 10-minute fixed-time windows.
*   The `PCollection` has a repeating trigger that fires every time 3 elements
    arrive.

The following diagram shows data events for key X as they arrive in the
PCollection and are assigned to windows. To keep the diagram a bit simpler,
we'll assume that the events all arrive in the pipeline in order.

![Diagram of data events for acculumating mode example]({{ "/images/trigger-accumulation.png" | prepend: site.baseurl }} "Data events for accumulating mode example")

##### 8.4.1.1. 累积模式 {#accumulating-mode}

If our trigger is set to accumulating mode, the trigger emits the following
values each time it fires. Keep in mind that the trigger fires every time three
elements arrive:

```
  First trigger firing:  [5, 8, 3]
  Second trigger firing: [5, 8, 3, 15, 19, 23]
  Third trigger firing:  [5, 8, 3, 15, 19, 23, 9, 13, 10]
```


##### 8.4.1.2. 丢弃模式 {#discarding-mode}

If our trigger is set to discarding mode, the trigger emits the following values
on each firing:

```
  First trigger firing:  [5, 8, 3]
  Second trigger firing:           [15, 19, 23]
  Third trigger firing:                         [9, 13, 10]
```

#### 8.4.2. 后期数据处理 {#handling-late-data}

> The Beam SDK for Python does not currently support allowed lateness.

If you want your pipeline to process data that arrives after the watermark
passes the end of the window, you can apply an *allowed lateness* when you set
your windowing configuration. This gives your trigger the opportunity to react
to the late data. If allowed lateness is set, the default trigger will emit new
results immediately whenever late data arrives.

You set the allowed lateness by using `.withAllowedLateness()` when you set your
windowing function:

```java
  PCollection<String> pc = ...;
  pc.apply(Window.<String>into(FixedWindows.of(1, TimeUnit.MINUTES))
                              .triggering(AfterProcessingTime.pastFirstElementInPane()
                                                             .plusDelayOf(Duration.standardMinutes(1)))
                              .withAllowedLateness(Duration.standardMinutes(30));
```
```py
  # The Beam SDK for Python does not currently support allowed lateness.
```

This allowed lateness propagates to all `PCollection`s derived as a result of
applying transforms to the original `PCollection`. If you want to change the
allowed lateness later in your pipeline, you can apply
`Window.configure().withAllowedLateness()` again, explicitly.


### 8.5. 组合触发器 {#composite-triggers}

You can combine multiple triggers to form **composite triggers**, and can
specify a trigger to emit results repeatedly, at most once, or under other
custom conditions.

#### 8.5.1. 组合触发器类型 {#composite-trigger-types}

Beam includes the following composite triggers:

*   You can add additional early firings or late firings to
    `AfterWatermark.pastEndOfWindow` via `.withEarlyFirings` and
    `.withLateFirings`.
*   `Repeatedly.forever` specifies a trigger that executes forever. Any time the
    trigger's conditions are met, it causes a window to emit results and then
    resets and starts over. It can be useful to combine `Repeatedly.forever`
    with `.orFinally` to specify a condition that causes the repeating trigger
    to stop.
*   `AfterEach.inOrder` combines multiple triggers to fire in a specific
    sequence. Each time a trigger in the sequence emits a window, the sequence
    advances to the next trigger.
*   `AfterFirst` takes multiple triggers and emits the first time *any* of its
    argument triggers is satisfied. This is equivalent to a logical OR operation
    for multiple triggers.
*   `AfterAll` takes multiple triggers and emits when *all* of its argument
    triggers are satisfied. This is equivalent to a logical AND operation for
    multiple triggers.
*   `orFinally` can serve as a final condition to cause any trigger to fire one
    final time and never fire again.

#### 8.5.2. 使用AfterWatermark组合 {#composite-afterwatermark}

Some of the most useful composite triggers fire a single time when Beam
estimates that all the data has arrived (i.e. when the watermark passes the end
of the window) combined with either, or both, of the following:

*   Speculative firings that precede the watermark passing the end of the window
    to allow faster processing of partial results.

*   Late firings that happen after the watermark passes the end of the window,
    to allow for handling late-arriving data

You can express this pattern using `AfterWatermark`. For example, the following
example trigger code fires on the following conditions:

*   On Beam's estimate that all the data has arrived (the watermark passes the
    end of the window)

*   Any time late data arrives, after a ten-minute delay

*   After two days, we assume no more data of interest will arrive, and the
    trigger stops executing

```java
  .apply(Window
      .configure()
      .triggering(AfterWatermark
           .pastEndOfWindow()
           .withLateFirings(AfterProcessingTime
                .pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(10))))
      .withAllowedLateness(Duration.standardDays(2)));
```
```py
pcollection | WindowInto(
    FixedWindows(1 * 60),
    trigger=AfterWatermark(
        late=AfterProcessingTime(10 * 60)),
    accumulation_mode=AccumulationMode.DISCARDING)
```

#### 8.5.3. 其他组合触发器 {#other-composite-triggers}

You can also build other sorts of composite triggers. The following example code
shows a simple composite trigger that fires whenever the pane has at least 100
elements, or after a minute.

```java
  Repeatedly.forever(AfterFirst.of(
      AfterPane.elementCountAtLeast(100),
      AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
```
```py
pcollection | WindowInto(
    FixedWindows(1 * 60),
    trigger=Repeatedly(
        AfterAny(
            AfterCount(100),
            AfterProcessingTime(1 * 60))),
    accumulation_mode=AccumulationMode.DISCARDING)
```