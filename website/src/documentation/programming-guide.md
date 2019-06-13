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

**Beam编程指南**适用于希望使用Beam SDK创建数据处理管道的Beam用户。 它为使用Beam SDK类构建和测试你的管道提供了指导。 它不是一个详细且全面的参考，而是一个与语言无关，以编程方式构建你的Beam管道的高级指南。 在编写本指南时，包含多种语言的代码示例，用来帮助说明如何在你的管道中实现Beam概念。

<nav class="language-switcher">
  <strong>适应于:</strong>
  <ul>
    <li data-type="language-java" class="active">Java SDK</li>
    <li data-type="language-py">Python SDK</li>
  </ul>
</nav>


## 1. 概述{#overview}

为了使用Beam，你首先需要选择一种Beam SDK，来创建一个驱动程序，它*定义*了你的管道，包括所有输入，变换和输出; 
它还为你的管道设置了执行选项（一般用在命令行操作）。 其中包括Pipeline Runner，它反过来确定你的管道将运行在哪个后端。


Beam SDK提供了许多抽象概念，简化了大规模分布式数据处理的机制。 相同的Beam抽象适用于批处理和流数据源。 当你创建一个Beam管道时，你可以根据这些抽象来考虑数据处理任务。 它们包括：

* `Pipeline`: 一个 `Pipeline`，从头到尾封装了你的整个数据处理任务。它包括读取输入数据，变换数据和写入输出数据。所有Beam驱动程序都必须创建一个 `Pipeline`。当你创建 `Pipeline`时，还必须指定执行选项，这些选项告诉 `Pipeline`，在何处以及如何运行。

* `PCollection`: 一个 `PCollection` 代表着你的Beam管道操作的分布式数据集。数据集可以是*有界*的，这意味着它来自像文件这样的固定源， 或者也可以是*无界*的，这意味着它来自通过订阅或其他机制不断更新的源。 你的管道通常通过从外部数据源读取数据来创建初始的 `PCollection`，但你也可以从驱动程序中的内存数据创建一个 `PCollection`。从那里开始， `PCollection` 将作为你管道中每个步骤的输入和输出。

* `PTransform`: 一个 `PTransform` 代表着你管道中的数据处理操作或步骤。 每个 `PTransform` 都将一个或多个 `PCollection` 对象作为输入，执行你在该
  `PCollection` 的元素上提供的处理函数，并生成零个或多个输出 `PCollection` 对象。

* I/O 转换: Beam附带了许多“IO”——`PTransform` 库，它可以读或写数据到各种外部存储系统。

一个典型的Beam驱动程序的工作原理如下：:

* **创建** 一个 `Pipeline` 对象并设置管道执行选项，包括Pipeline Runner。
* 为管道数据创建初始 `PCollection`， 使用IO从外部存储系统读取数据，或使用 `Create` 转换从内存数据中构建的一个 `PCollection`。
* **应用** 将 `PTransforms` 应用于每个 `PCollection`。变换可以更改，过滤，分组，分析或以其他方式处理 `PCollection` 中的元素。 转换在*不修改输入集合*的情况下，创建一个新的输出 `PCollection`。 一个典型的管道依次对每个新的输出 `PCollection` 应用后续转换，直到处理完成。 请注意，管道不一定是一个接一个应用的单个直线转换：将 `PCollection` 视为变量，将 `PTransform` 视为应用于这些变量的函数：管道的形状可以是任意复杂的处理图。
* 使用IO，将最终的转换后的 `PCollection` 写入外部源。
* **运行** 管道， 使用指派的 Pipeline Runner。

当你运行你的Beam驱动程序时，你指派的Pipeline Runner会根据你创建的 `PCollection`对象和应用的转换为你的管道构建一个**工作流图** 。然后使用适当的分布式处理后端执行该图，从而成为该后端的异步“作业”(或等效的)。

## 2. 创建一个管道 {#creating-a-pipeline}

`Pipeline` 抽象封装了你数据处理任务中的所有数据和步骤。  你的Beam驱动程序通常首先构建一个
[Pipeline](https://beam.apache.org/releases/javadoc/2.12.0/index.html?org/apache/beam/sdk/Pipeline.html) [Pipeline](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/pipeline.py) 对象, ，然后使用该对象作为基础， 用于将管道的数据集创建为 `PCollection`，并将它的操作创建为 `Transform`。

为了使用Beam，你的驱动程序必须首先创建Beam SDK类 `Pipeline` 的实例 (通常在 `main()` 函数中). 当你创建你的 `Pipeline` 时，你还需要设置一些**配置选项**。 你可以以编程方式设置管道的配置选项，, 但通常更容易的做法是提前设置选项（或从命令行读取它们），并在创建对象时将它们传递给 `Pipeline` 对象。

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

使用管道选项来配置你管道的不同方面，例如，将执行管道的管道运行程序以及所选运行程序所需的任何特定于运行程序的配置。 你的管道选项可能包含诸如项目ID或存储文件的位置之类的信息。

当在你选择的运行器上运行管道时，你的代码将可以使用PipelineOptions的副本。例如，如果将一个PipelineOptions参数添加到DoFn的 `@ProcessElement` 方法，它将由系统填充。

#### 2.1.1. 从命令行参数设置PipelineOptions {#pipeline-options-cli}

虽然你可以通过创建一个 `PipelineOptions` 对象并直接设置字段来配置管道，但Beam SDK包含一个命令行解析器，你可以通过它使用命令行参数在 `PipelineOptions` 中设置字段。

为了从命令行中读取选项，请构造你的 `PipelineOptions` 对象，如以下示例代码所示：

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

以这种方式构建你的 `PipelineOptions`，允许你将任何选项指定为命令行参数。

> **注意:** [WordCount管道示例](https://beam.apache.org/get-started/wordcount-example/) 演示了如何使用命令行选项在运行时设置管道选项。

#### 2.1.2. 创建自定义选项 {#creating-custom-options}

除标准 `PipelineOptions` 外，你还可以添加你自己的自定义选项。为了添加你自己的选项，请为每个选项定义带有getter和setter方法的接口，如下面的示例所示，用于添加 `input` 和 `output` 自定义选项:

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

你还可以指定一个描述，该描述在用户将 `--help` 作为命令行参数传递时显示，并显示默认值。

你可以使用注解设置描述和默认值，如下所示：

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
我们建议你使用 `PipelineOptionsFactory` 注册你的接口，然后在创建 `PipelineOptions` 对象时传递该接口。 当你使用 `PipelineOptionsFactory` 注册你的接口时，`--help` 可以找到你的自定义选项接口并将它添加到 `--help` 命令的输出中。 `PipelineOptionsFactory` 还将验证你的自定义选项是否与所有其他已注册的选项兼容。


下面的示例代码，显示了如何使用 `PipelineOptionsFactory` 来注册你的自定义选项接口：

```java
PipelineOptionsFactory.register(MyOptions.class);
MyOptions options = PipelineOptionsFactory.fromArgs(args)
                                                .withValidation()
                                                .as(MyOptions.class);
```

现在，你的管道可以接受 `--input=value` 和 `--output=value` 作为命令行参数。

## 3. PCollections {#pcollections}

[PCollection](https://beam.apache.org/releases/javadoc/2.12.0/index.html?org/apache/beam/sdk/values/PCollection.html)
`PCollection` 抽象表示一个潜在分布的，多元素数据集。 你可以将 `PCollection` 视为“管道”数据; Beam的变换使用 `PCollection` 对象作为输入和输出。  因此，如果你要处理你管道中的数据，就必须采用 `PCollection` 的形式。

在你创建你的 `Pipeline` 之后，你首先需要以某种形式创建至少一个  `PCollection` 。 你创建的  `PCollection` 将用作管道中第一个操作的输入。

### 3.1. 创建一个PCollection {#creating-a-pcollection}

你可以通过使用Beam的[Source API](#pipeline-io)从外部源读取数据来创建一个 `PCollection` ，也可以在你的驱动程序中，创建一个存储在内存集合类中的数据的 `PCollection` 。前者通常是如何获取数据的生产管道; Beam的Source API包含了适配器，可以帮助你从外部源读取大型基于云的文件，数据库或订阅服务。后者主要是用于测试和调试的目的。

#### 3.1.1. 从外部源读取 {#reading-external-source}

要从外部源读取数据，请使用Beam提供的一个[I/O适配器](#pipeline-io)。 每个适配器的确切用法各不相同，但它们都从一些外部数据源读取数据，并返回一个表示该源中数据记录的 `PCollection` 元素。

每个数据源适配器都有一个 `Read` 变换;要读取，你必须将该变换应用于 `Pipeline` 对象本身。例如，`TextIO.Read`，`io.TextFileSource` 从外部文本文件读取并返回元素类型为 `String` 的 `PCollection`，每个 `String` 表示文本文件中的一行。 下面是将`TextIO.Read`，`io.TextFileSource` 应用于管道以创建 `PCollection` 的方法：

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


要从内存中的Java `Collection` 中创建一个 `PCollection`，请使用Beam提供的 `Create` 转换。与数据适配器的 `Read` 非常相似，你可以直接将 `Create` 应用于 `Pipeline` 对象本身。

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

一个 `PCollection` 的大小可以是**有界的**，也可以是**无界的**。 一个**有界**的 `PCollection` 表示已知固定大小的数据集，而**无界** `PCollection` 表示无限大小的数据集。一个 `PCollection` 是有界的还是无界的取决于它所表示的数据集的源。. 从批处理数据源（例如文件或数据库）读取数据会创建一个有界 `PCollection`。  从流式传输或不断更新的数据源（如发布/订阅或Kafka）中读取，会创建一个无界的 `PCollection`（除非你明确告诉它不要这样做）。

你的 `PCollection` 的有界（或无界）性质会影响Beam处理数据的方式。可以使用批处理作业处理一个有界 `PCollection`，批处理作业可以一次读取整个数据集，并在有限长度的作业中执行处理。必须使用连续运行的流式作业处理无界 `PCollection` ，因为在任何时候都不能对整个集合进行处理。

Beam使用[窗口](#windowing)将一个不断更新的无界 `PCollection` 划分为有限大小的逻辑窗口。这些逻辑窗口由与数据元素相关联的某些特征确定，例如**时间戳**。聚合转换（例如 `GroupByKey` 和 `Combine` ）在每个窗口的基础上工作 —当生成数据集时，他们将每个 `PCollection` 作为这些有限窗口的连续处理。


#### 3.2.5. 元素时间戳 {#element-timestamps}

在一个 `PCollection` 中的 每个元素都有一个关联的固有**时间戳**。 每个元素的时间戳最初由创建 `PCollection` 的[Source](#pipeline-io)分配。 创建无界 `PCollection` 的源通常会为每个新元素分配一个时间戳，该时间戳对应于读取或添加元素的时间。

> **注意**: 为固定数据集创建有界 `PCollection` 的源也会自动分配时间戳，但最常见的行为是为每个元素分配相同的时间戳（`Long.MIN_VALUE`）。

时间戳对于一个包含具有固有时间概念的元素的 `PCollection` 非常有用。如果你的管道正在读取事件流（如Tweets或其他社交媒体消息），那么每个元素可能会使用事件发布的时间作为元素时间戳。

如果数据源不为你执行此操作，你可以手动将时间戳分配给一个 `PCollection` 的元素。 如果元素有一个固有的时间戳，则你希望这样做，但时间戳位于元素本身的结构中（例如服务器日志条目中的“time”字段）的某个位置。Beam具有将一个 `PCollection` 作为输入和输出的[变换](#transforms)，该转换与附加了时间戳的 `PCollection` 相同; 有关如何执行此操作的详细信息，请参阅[添加时间戳](#adding-timestamps-to-a-pcollections-elements)。

## 4. 变换 {#transforms}

变换是你管道中的操作，并提供一个通用的处理框架。  你以函数对象的形式提供处理逻辑（通俗地称为“用户代码”），并且你的用户代码应用于输入 `PCollection`（或多个 `PCollection` ）的每个元素。
根据你选择的管道运行程序和后端，集群中的许多不同工作者可以并行执行用户代码的实例。在每个工作者上运行的用户代码生成输出元素，这些元素最终被添加到变换产生的最终输出 `PCollection` 中。

Beam SDK包含许多不同的变换，你可以将这些变换应用于管道的 `PCollection`。 这些变换包括通用核心变换，例如[ParDo](#pardo)或[Combine](#combine)。 SDK中还包含预先编写的[组合变换](#composite-transforms)，它们以一种有用的处理模式组合一个或多个核心变换，例如计算或组合集合中的元素。 你还可以定义自己更复杂的组合变换，以适应管道的确切用例。

### 4.1. 应用变换 {#applying-transforms}

要调用一个变换，必须将它**应用**于输入 `PCollection`。Beam SDK中的每个转换都有一个通用的 `apply` 方法（或管道运算符 `|`）。 调用多个Beam变换类似于*方法链接*，但有一点不同：将变换应用于输入 `PCollection` 时，将变换本身作为参数传递，然后操作返回输出 `PCollection`。它的一般形式如下：

```java
[Output PCollection] = [Input PCollection].apply([Transform])
```
```py
[Output PCollection] = [Input PCollection] | [Transform]
```

因为Beam对 `PCollection` 使用了一个通用的 `apply` 方法，所以你既可以按顺序链接变换，也可以应用包含嵌套在其中的其他变换的变换（在Beam SDK中称为[组合变换](#composite-transforms)）。

你如何应用你管道的转换决定了你管道的结构。思考你管道的最佳方法是将其看作一个有向无环图，其中节点是 `PCollection`，边是变换。例如，你可以链接变换来创建一个顺序的管道，如下所示:

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

但是，请注意，转换*不会消耗或以其他方式更改*输入集合 - 请记住，一个 `PCollection` 根据定义是不可变的。 这意味着你可以将多个变换应用于同一个输入 `PCollection` 来创建分支管道，如下所示： 

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

你还可以构建你自己的[组合转换](#composite-transforms)，将多个子步骤嵌套在一个更大的转换中。组合转换对于构建可重用的简单步骤序列特别有用，这些步骤可以在许多不同的地方使用。

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

`ParDo` 是一种用于通用并行处理的Beam变换。`ParDo` 处理范例类似于Map/Shuffle/Reduce风格算法的"Map"阶段：一个 `ParDo` 变换考虑输入 `PCollection` 中的每个元素，对该元素执行一些处理功能（你的用户代码），并发出零 ，一个或多个元素到输出 `PCollection`。

`ParDo` 对于各种常见的数据处理操作非常有用，包括：

* **过滤数据集。** 你可以使用 `ParDo` 考虑  `PCollection` 中的每个元素，并将该元素输出到一个新集合，或将它丢弃。
* **格式化或类型转换数据集中的每个元素。** 如果你的输入 `PCollection` 包含的元素类型或格式与你想要的不同，则可以使用 `ParDo` 对每个元素执行转换，并将结果输出到一个新的 `PCollection`。
* **提取数据集中每个元素的部分。** 例如，如果你有一个包含多个字段的 `PCollection` 记录， 则可以使用 `ParDo` 将要考虑的字段解析为一个新的 `PCollection`。
* **对数据集中的每个元素执行计算。** 你可以使用 `ParDo` 对 `PCollection` 的每个元素或某些元素执行简单或复杂的计算，并将结果输出为新的 `PCollection`。

在这些角色中，`ParDo` 是管道中常见的中间步骤。你可以使用它从一组原始输入记录中提取某些字段，或将原始输入转换为其他格式;你也可以使用 `ParDo` 将处理后的数据转换为适合输出的格式，如数据库表行或可打印字符串。

当你应用一个 `ParDo` 转换时，你需要以 `DoFn` 对象的形式提供用户代码。 `DoFn` 是一个Beam SDK类，它定义了分布式处理函数。

>当你创建 `DoFn` 的子类时, 请注意，你的子类应该遵守[为Beam变换编写用户代码的要求](#requirements-for-writing-user-code-for-beam-transforms).

##### 4.2.1.1. 应用ParDo {#applying-pardo}

与所有Beam变换一样，你通过在输入 `PCollection` 上调用 `apply` 方法来应用 `ParDo`，
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

传递给 `ParDo` 的 `DoFn` 对象包含应用于输入集合中的元素的处理逻辑。你将编写的最重要的代码片段通常是这些 `DoFn`——它们定义了管道的确切数据处理任务。.

> **注意:** 当你创建你的 `DoFn` 时，请注意[为Beam变换编写用户代码的要求](#requirements-for-writing-user-code-for-beam-transforms)，并确保你的代码遵循这些要求。


一个 `DoFn` 每次处理输入 `PCollection` 中的一个元素。 当你在创建 `DoFn` 的子类时，你需要提供与输入和输出元素的类型匹配的类型参数。如果你的 `DoFn` 处理传入的 `String` 元素并为输出集合生成 `Integer` 元素（就像我们之前的示例`ComputeWordLengthFn`），你的类声明将如下所示：

```java
static class ComputeWordLengthFn extends DoFn<String, Integer> { ... }
```

在你的 `DoFn` 子类中, 你将编写一个带有 `@ProcessElement` 注解的方法，在这个方法中，你可以提供实际的处理逻辑。你不需要手动从输入集合中提取元素;Beam SDKs会为你处理。你的 `@ProcessElement` 方法应该接受一个带有 `@Element` 标记的参数，该参数将使用input元素填充。为了输出元素，该方法还可以采用 `OutputReceiver`类型的参数，该参数提供用于发出元素的方法。参数类型必须匹配 `DoFn` 的输入和输出类型，否则框架将引发错误。 注意：@Llement和OutputReceiver是在Beam 2.5.0中引入的;如果使用早期版本的Beam，则应使用ProcessContext参数。


在你的 `DoFn` 子类中，你将编写一个方法 `process`，你可以在其中提供实际的处理逻辑。 你不需要从输入集合中手动提取元素; Beam SDKs会为你处理。 你的 `process` 方法应该接受 `element` 类型的对象。 这是输入元素，并使用 `yield` 或 `return` 语句在 `process` 方法内部发出输出。

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

通常会调用给定的 `DoFn` 实例一次或多次来处理某些任意元素束。但是，Beam不保证确切的调用次数;可以在给定的工作节点上多次调用它以解决故障和重试。因此，你可以在处理方法的多个调用之间缓存信息，但是如果这样做，请确保实现**不依赖于调用的次数**。

在你的处理方法中，你还需要满足一些不变性要求，以确保Beam和处理后端可以安全地序列化并缓存管道中的值。 你的方法应符合以下要求：

* 你不应以任何方式修改 `@Element` 注解或 `ProcessContext.sideInput()` (输入集合中的传入元素)返回的元素.
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

如果你的 `ParDo` 执行输入元素与输出元素的一对一映射 - 也就是说，对于每个输入元素，它应用一个*恰好生成一个*输出元素的函数，则可以使用更高级别的 `MapElements` `Map` 变换。 为了更加简洁， `MapElements` 可以接受匿名的Java 8 lambda函数。

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
> **注意：** 你可以将Java 8 lambda函数与其他几个Beam变换一起使用，包括 `Filter`, `FlatMapElements`, 和 `Partition`。

#### 4.2.2. GroupByKey {#groupbykey}

`GroupByKey`是用于处理键/值对集合的Beam变换。 这是一个并行归约操作，类似于Map/Shuffle/Reduce风格算法的Shuffle阶段。`GroupByKey` 的输入是表示*多映射*的键/值对的集合，其中集合包含多个具有相同键但不具有相同值的对。给定这样一个集合，你可以使用   `GroupByKey`收集与每个惟一键关联的所有值。

`GroupByKey` 是一种聚合某些有共同点的数据的好方法。例如，如果你有一个存储客户订单记录的集合，你可能希望将来自相同邮政编码的所有订单组合在一起（其中键/值对的“键”是邮政编码字段，并且“ 值“是记录的剩余部分）。

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

如果你使用无界 `PCollection`，则必须使用[非全局窗口](#setting-your-pcollections-windowing-function)或[聚合触发器](#triggers)才能执行 `GroupByKey` 或[CoGroupByKey](#cogroupbykey)。 这是因为有界 `GroupByKey` 或 `CoGroupByKey` 必须等待收集某个键的所有数据，但是对于无界集合，数据是无限的。窗口和/或触发器允许对无界数据流中的逻辑、有限的数据束进行分组操作。

如果你将 `GroupByKey` 或 `CoGroupByKey` 应用于一组无界 `PCollection` ，而没有为每个集合设置非全局窗口策略，触发策略或同时设置这两种策略，则Beam会在管道构建时生成IllegalStateException错误。

使用 `GroupByKey` 或 `CoGroupByKey` 对应用了[窗口策略](#windowing)的 `PCollection` 进行分组时，要分组的所有 `PCollection` *必须使用相同的窗口策略*和窗口大小。 例如，你要合并的所有集合必须使用（假设）相同的5分钟固定窗口，或每30秒开始的一次4分钟滑动窗口。

如果你的管道尝试使用 `GroupByKey` 或 `CoGroupByKey` 将 `PCollection`与不兼容的窗口合并，则Beam会在管道构建时生成IllegalStateException错误。

#### 4.2.3. CoGroupByKey {#cogroupbykey}

`CoGroupByKey` 执行具有相同键类型的两个或多个键/值 `PCollection` 的关系连接。[设计你的管道](https://beam.apache.org/documentation/pipelines/design-your-pipeline/#multiple-sources)显示了一个使用连接的示例管道。

如果你有多个数据集来提供有关相关事物的信息，请考虑使用`CoGroupByKey`。例如，假设你有两个不同的用户数据文件：一个文件有名称和电子邮件地址; 另一个文件有姓名和电话号码。 你可以使用用户名作为公共键，并将其他数据作为关联值，来连接这两个数据集。 在连接之后，你有一个数据集，其中包含与每个名称关联的所有信息（电子邮件地址和电话号码）。

如果你使用的是无界 `PCollection`，则必须使用[非全局窗口](#setting-your-pcollections-windowing-function)或[聚合触发器](#triggers)才能执行一个 `CoGroupByKey`。有关详细信息，请参阅[GroupByKey和无界PCollections](#groupbykey-and-unbounded-pcollections)。


在Java的Beam SDK中，`CoGroupByKey` 接受一个以 `PCollection` 为键的元组 (`PCollection<KV<K, V>>`)作为输入。为了类型安全，SDK要求你将每个 `PCollection` 作为 `KeyedPCollectionTuple` 的一部分传递。你必须为要传递给 `CoGroupByKey` 的 `KeyedPCollectionTuple` 中的每个输入 `PCollection` 声明一个  `TupleTag`。作为输出，`CoGroupByKey` 返回一个 `PCollection<KV<K, CoGbkResult>>`，它根据所有输入 `PCollection` 的公共键对值进行分组。 每个键（所有类型 `K`）将具有不同的 `CoGbkResult`，这是从 `TupleTag<T>` 到 `Iterable<T>`的映射。你可以使用一个随初始集合提供的 `TupleTag` 来访问 `CoGbkResult` 对象中的特定集合。

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

当你应用 `Combine` 变换时，必须提供包含组合元素或值的逻辑的函数。 组合函数应该是可交换的和关联的，因为对于给定键的所有值，函数不一定都只调用一次。 由于输入数据（包括值集合）可以分布在多个工作者之间，所以可以多次调用组合函数来对值集合的子集执行部分组合。 Beam SDK还为常见的数字组合操作（如sum，min和max）提供了一些预构建的组合函数。

简单的组合操作（例如求和）通常可以作为一个简单的函数来实现。更复杂的组合操作可能需要你创建 `CombineFn`  的子类，它具有与输入/输出类型不同的累积类型。

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

对于更复杂的组合函数，你可以定义 `CombineFn` 的子类。 如果组合函数需要更复杂的累加器，必须执行额外的预处理或后处理，可能会更改输出类型或者把键考虑在内，则你应该使用`CombineFn`。

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

如果你要组合一个键值对的 `PCollection`，则每个[键组合](#combining-values-in-a-keyed-pcollection)通常就足够了。如果你需要根据键更改组合策略（例如，某些用户为MIN，其他用户为MAX），则可以定义 `KeyedCombineFn` 来访问组合策略中的键。

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

如果你的输入 `PCollection` 使用默认的全局窗口，则默认行为是返回包含一个项的 `PCollection`。 该项的值来自你在应用 `Combine` 时指定的组合函数中的累加器。 例如，Beam提供的sum组合函数返回一个零值（一个空输入的求和），而min组合函数返回最大值或无穷大值。

要使 `Combine` 在输入为空时返回空的 `PCollection`，请在你应用的 `Combine` 变换时指定 `.withoutDefaults` ，如下面的代码示例所示：

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

如果你的 `PCollection` 使用任何非全局窗口函数，则Beam不会提供默认行为。 应用 `Combine` 时，你必须指定以下选项之一：
* 指定 `.withoutDefaults`，其中在输入 `PCollection` 中为空的窗口，将在输出集合中同样为空。
* 指定 `.asSingletonView`，其中输出立即转换为 `PCollectionView`，当用作侧输入时，它将为每个空窗口提供默认值。 如果你管道的 `Combine` 结果在后面的管道中用作侧输入时，才需要使用这个选项。

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

在上面的 `PCollection` 中，每个元素都有一个字符串键（例如，“cat”）和一个可迭代的整数值（在第一个元素中，包含[1,5,9]）。 如果我们管道的下一个处理步骤组合了这些值（而不是单独考虑它们），你可以组合可迭代的整数来创建一个单个的，合并的值，以便与每个键配对。这种之后合并值集合 `GroupByKey` 模式，相当于Beam的Combine PerKey变换。 你提供给Combine PerKey的组合函数必须是关联归约函数或 `CombineFn` 的一个子类。

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

默认情况下，输出 `PCollection` 的编码器与输入  `PCollectionList` 中第一个 `PCollection`的编码器相同。 但是，输入的 `PCollection` 对象可以使用不同的编码器，只要它们都包含你选择的语言中相同的数据类型即可。

##### 4.2.5.2. 合并窗口集合 {#merging-windowed-collections}

当使用 `Flatten` 合并应用了窗口策略的 `PCollection` 对象时，要合并的所有 `PCollection` 对象必须使用兼容的窗口策略和窗口大小。 例如，你正在合并的所有集合必须全部使用（假设）相同的5分钟固定窗口或每30秒开始一次的4分钟滑动窗口。

如果你的管道尝试使用 `Flatten` 将 `PCollection` 对象与不兼容的窗口合并，则在构建管道时，Beam会生成一个  `IllegalStateException` 错误。

#### 4.2.6. 分区 {#partition}

[`Partition`](https://beam.apache.org/releases/javadoc/2.13.0/index.html?org/apache/beam/sdk/transforms/Partition.html) [`Partition`](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/core.py) 是存储相同数据类型的 `PCollection` 对象的Beam变换。`Partition` 将单个 `PCollection` 拆分为固定数量的较小集合。

`Partition` 根据你提供的分区函数划分一个 `PCollection` 的元素。 分区函数包含确定如何将输入 `PCollection` 的元素拆分为每个结果分区 `PCollection` 的逻辑。 必须在图构建时确定分区数。 例如，你可以在运行时将分区数作为命令行选项传递（它之后将被用于构建管道图），但是你无法确定管道中间的分区数（例如，根据你管道图构建后计算的数据）。

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

// 你可以使用get方法从PCollectionList中提取每个分区，如下所示：
PCollection<Student> fortiethPercentile = studentsByPercentile.get(4);
```
```py
# 提供一个int值，其中包含所需的结果分区数量，以及一个分区函数(本例中为partition_fn)。
# 返回一个PCollection对象的元组，其中包含作为单个PCollection对象的每个结果分区。
students = ...
def partition_fn(student, num_partitions):
  return int(get_percentile(student) * num_partitions / 100)

by_decile = students | beam.Partition(partition_fn, 10)

# 你可以从PCollection对象的元组中提取每个分区，如下所示：
fortieth_percentile = by_decile[4]
```

### 4.3. 为Beam变换编写用户代码的要求 {#requirements-for-writing-user-code-for-beam-transforms}

当你为一个Beam变换构建用户代码时，应记住执行的分布式特性。 例如，可能有许多函数的副本在许多不同的机器上并行运行，这些副本可能独立运行，不与任何其他副本通信或共享状态。 根据你为你管道选择的管道运行器和处理后端，你的用户代码函数副本可能被重试或运行多次。 因此，你应该谨慎地在用户代码中包含状态依赖之类的内容。

通常，你的用户代码必须至少满足以下要求：

* 你的函数对象必须是**可序列化的**。
* 你的函数对象必须是**线程兼容**的，并且要注意Beam SDK不是线程安全的。

此外，我们建议你让你的函数对象具有**幂等性**。非幂等函数由Beam支持，但当存在外部副作用时，需要额外的考虑来确保正确性。

> **注意：** 这些要求适用于 `DoFn`（与[ParDo](#pardo)变换一起使用的函数对象），`CombineFn`（与[Combine](#combine)变换一起使用的函数对象）和 `WindowFn`（与[Window](#windowing)变换一起使用的函数对象）的子类。

#### 4.3.1. 可串行化 {#user-code-serializability}

你为一个变换提供的任何函数对象都必须是**完全可序列化**的。 这是因为需要将函数的副本序列化并传输到处理集群中的远程工作程序。 用户代码的基类，如 `DoFn`，`CombineFn` 和 `WindowFn`，已实现 `Serializable`; 但是，你的子类不能添加任何不可序列化的成员。

你应该记住的一些其他可串行化因素是：

* 函数对象中的瞬态字段*不会*传输到工作者实例中，因为它们不会自动序列化。
* 在序列化之前避免加载包含有大量数据的字段。
* 函数对象的各个实例不能共享数据。
* 在函数对象被应用后对它进行的改变将不起作用。
* 使用匿名内部类实例在内联声明函数对象时要小心。 在非静态上下文中，内部类实例将隐式包含指向封闭类和该类的状态的指针。 这个封闭类也将被序列化，因此应用于函数对象本身的相同注意事项也适用于这个外部类。

#### 4.3.2. 线程兼容 {#user-code-thread-compatibility}

你的函数对象应该与线程兼容。 除非你明确地创建自己的线程，否则每个函数对象的实例都会在工作者实例上由单个线程访问一次。 但是，请注意，Beam SDKs不是线程安全的。 如果在用户代码中创建自己的线程，则必须提供自己的同步。 请注意，函数对象中的静态成员不会传递给工作者实例，而且可以从不同的线程访问函数的多个实例。

#### 4.3.3. 幂等性 {#user-code-idempotence}

建议你使你的函数对象具有幂等性 - 也就是说，它可以根据需要重复或重试，而不会导致意外的副作用。 支持非幂等函数，但Beam模型不保证可以调用或重试用户代码的次数; 因此，保持函数对象的幂等性可以使管道输出具有确定性，并且你的变换行为更易于预测且更易于调试。

### 4.4. 侧输入 {#side-inputs}

除了主输入 `PCollection` 之外，你还可以以侧输入的形式为一个 `ParDo` 变换提供额外的输入。 侧输入是一个额外输入，`DoFn` 每次处理输入 `PCollection` 中的一个元素时，都可以访问它。 当你指定一个侧输入时，你将创建一些其他数据的视图，这些数据可以在处理每个元素时从 `ParDo` 变换的  `DoFn` 中读取。

如果你的 `ParDo` 在处理输入 `PCollection` 中的每个元素时，都需要注入额外数据，则侧输入很有用，但这些额外数据需要在运行时确定（而不是硬编码）。 这些值可能由输入数据确定，或取决于管道的不同分支。


#### 4.4.1. 将侧输入传递给ParDo {#side-inputs-pardo}

```java
  // 通过调用.withSideInputs将你的侧输入传递给ParDo转换。
  // 在你的DoFn内部，使用方法DoFn.ProcessContext.sideInput访问侧输入。

  // 输入PCollection到ParDo。
  PCollection<String> words = ...;

  // 单词长度的PCollection，我们将组合成单个值。
  PCollection<Integer> wordLengths = ...; // 单个PCollection

  // 使用Combine.globally和View.asSingleton从wordLengths创建一个单例PCollectionView。
  final PCollectionView<Integer> maxWordLengthCutOffView =
     wordLengths.apply(Combine.globally(new Max.MaxIntFn()).asSingletonView());


  // 应用一个将maxWordLengthCutOffView作为侧输入的ParDo。
  PCollection<String> wordsBelowCutOff =
  words.apply(ParDo
      .of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
            // 在我们的DoFn中，访问侧输入
            int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
            if (word.length() <= lengthCutOff) {
              out.output(word);
            }
          }
      }).withSideInputs(maxWordLengthCutOffView)
  );
```
```py
# 侧输入在DoFn的过程方法或Map/FlatMap的可调用中可用作额外参数。
# 可选参数，位置参数和关键字参数都支持。 延迟参数将被解包为它们的实际值。
# 例如，在管道构造时使用pvalue.AsIteor(pcoll)会导致pcoll的实际元素可迭代传递到每个流程调用中。
#在此示例中，侧输入作为额外参数传递给FlatMap变换，并由filter_using_length使用。
words = ...
# Callable需要额外的参数。
def filter_using_length(word, lower_bound, upper_bound=float('inf')):
  if lower_bound <= len(word) <= upper_bound:
    yield word

# 构造一个延迟的侧输入。
avg_word_len = (words
                | beam.Map(len)
                | beam.CombineGlobally(beam.combiners.MeanCombineFn()))

# 使用明确的侧输入调用。
small_words = words | 'small' >> beam.FlatMap(filter_using_length, 0, 3)

# 单个延迟的侧输入。
larger_than_average = (words | 'large' >> beam.FlatMap(
    filter_using_length,
    lower_bound=pvalue.AsSingleton(avg_word_len)))

# 混合和匹配。
small_but_nontrivial = words | beam.FlatMap(
    filter_using_length,
    lower_bound=2,
    upper_bound=pvalue.AsSingleton(avg_word_len))

# 我们还可以将侧输入传递给一个ParDo变换，该变换将传递给它的process方法。
# process方法的前两个参数是self和element。
class FilterUsingLength(beam.DoFn):
  def process(self, element, lower_bound, upper_bound=float('inf')):
    if lower_bound <= len(element) <= upper_bound:
      yield element

small_words = words | beam.ParDo(FilterUsingLength(), 0, 3)
...
```

#### 4.4.2. 侧输入和窗口 {#side-inputs-windowing}

一个窗口化 `PCollection` 可能是无限的，因此不能压缩为单个值（或单个集合类）。 当你为一个窗口化的 `PCollection` 创建一个 `PCollectionView` 时，`PCollectionView` 表示每个窗口的一个实体（每个窗口一个单例，每个窗口一个列表等）。

Beam使用主输入元素的窗口来查找侧输入元素的适当窗口。 Beam将主输入元素的窗口投影到侧输入的窗口集中，然后使用来自结果窗口中的侧输入。 如果主输入和侧输入具有相同的窗口，则投影提供精确的对应窗口。 然而，如果输入具有不同的窗口，则Beam使用投影选择最合适的侧输入窗口。

例如，如果使用一分钟的固定时间窗口对主输入进行窗口化，并且使用一小时的固定时间窗口对侧面输入进行窗口化，则Beam将主输入窗口投影到侧面输入窗口设置并从适当的时长侧输入窗口中选择侧输入值。

如果主输入元素存在于多个窗口中，则会多次调用 `processElement`，每窗口调用一次。 每次调用 `processElement` 都会为主输入元素投影“current”窗口，因此每次都可能提供不同的侧输入视图。

如果侧输入有多个触发器触发，则Beam使用最新触发器触发的值。 如果你使用带有单个全局窗口的侧输入并指定触发器，则这个功能特别有用。

### 4.5. 额外的输出 {#additional-outputs}

虽然 `ParDo` 始终生成一个主输出 `PCollection`（作为 `apply` 的返回值），但你也可以让 `ParDo` 生成任意数量的额外输出 `PCollection` 。 如果您选择具有多个输出，则 `ParDo` 将返回捆绑在一起的所有输出`PCollection`（包括主输出）。

#### 4.5.1. 用于多个输出的标签 {#output-tags}

```java
// 为了将元素发出到多个输出PCollections，请创建一个TupleTag对象以标识ParDo生成的每个集合。
// 例如，如果你的ParDo产生三个输出PCollections（主输出和两个额外输出），则必须创建三个TupleTag。
// 以下示例代码显示如何为具有三个输出PCollections的ParDo创建TupleTag。

  // 输入PCollection到我们的ParDo。
  PCollection<String> words = ...;

  // ParDo将过滤长度低于截止值的单词，并将它们添加到主输出PCollection<String>。
  // 如果一个单词高于截止值，ParDo会将字长添加到输出PCollection<Integer>。
  // 如果一个单词以字符串“MARKER”开头，则ParDo会将该单词添加到输出PCollection <String>。
  final int wordLengthCutOff = 10;

  // 创建三个TupleTag，每个输出PCollection一个。
  // 包含低于长度截止值的单词的输出。
  final TupleTag<String> wordsBelowCutOffTag =
      new TupleTag<String>(){};
  // 包含字长的输出。
  final TupleTag<Integer> wordLengthsAboveCutOffTag =
      new TupleTag<Integer>(){};
  // 包含“MARKER”单词的输出。
  final TupleTag<String> markedWordsTag =
      new TupleTag<String>(){};

// 将输出标签传递给ParDo：
// 为你的每个ParDo输出指定TupleTags后，通过调用.withOutputTags将标记传递给ParDo。
// 首先传递主输出的标记，然后传递TupleTagList中任何其他输出的标记。
// 请注意，所有输出（包括主输出PCollection）都绑定到返回的PCollectionTuple中。

  PCollectionTuple results =
      words.apply(ParDo
          .of(new DoFn<String, String>() {
            // DoFn继续在这里。
            ...
          })
          // 指定主输出的标记。
          .withOutputTags(wordsBelowCutOffTag,
          // 将两个额外输出的标记指定为TupleTagList。
                          TupleTagList.of(wordLengthsAboveCutOffTag)
                                      .and(markedWordsTag)));
```

```py
# 要将元素发射到多个输出PCollections，请在ParDo上调用with_outputs()，并为输出指定预期的标记。
# with_outputs()返回一个DoOutputsTuple对象。在with_outputs中指定的标记是返回的DoOutputsTuple对象的特性。
# 标签可以访问相应的输出PCollections。

results = (words | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x')
           .with_outputs('above_cutoff_lengths', 'marked strings',
                         main='below_cutoff_strings'))
below = results.below_cutoff_strings
above = results.above_cutoff_lengths
marked = results['marked strings']  # 索引也有效

# 结果也是可迭代的，按照与标签传递给with_outputs()的顺序相同的顺序排序，首先是主标记（如果指定）。

below, above, marked = (words
                        | beam.ParDo(
                            ProcessWords(), cutoff_length=2, marker='x')
                        .with_outputs('above_cutoff_lengths',
                                      'marked strings',
                                      main='below_cutoff_strings'))

```
#### 4.5.2. 在你的DoFn中发送多个输出 {#multiple-outputs-dofn}

```java
// 在ParDo的DoFn中，您可以通过向process方法提供一个MultiOutputReceiver并传入适当的TupleTag来获取一个OutputReceiver，从而向特定的输出PCollection发出一个元素。
// 在执行ParDo之后，从返回的PCollectionTuple中提取结果输出PCollections。
// 根据前面的示例，这显示了发送到主输出和两个额外输出的DoFn。

  .of(new DoFn<String, String>() {
     public void processElement(@Element String word, MultiOutputReceiver out) {
       if (word.length() <= wordLengthCutOff) {
         // 向主输出发出简短的单词。
         // 在此示例中，它是带有标签wordsBelowCutOffTag的输出。
         out.get(wordsBelowCutOffTag).output(word);
       } else {
         // 使用标签wordLengthsAboveCutOffTag将长单词发送到输出。
         out.get(wordLengthsAboveCutOffTag).output(word.length());
       }
       if (word.startsWith("MARKER")) {
         // 使用标签markedWordsTag向输出发出单词。
         out.get(markedWordsTag).output(word);
       }
     }}));
```

```py
# 在ParDo的DoFn中，您可以通过包装值和输出标记（str）将元素发送到特定输出。
# 使用pvalue.OutputValue包装类。
# 基于前面的示例，这显示了发送到主输出和两个额外输出的DoFn。

class ProcessWords(beam.DoFn):

  def process(self, element, cutoff_length, marker):
    if len(element) <= cutoff_length:
      # 将这个简短的单词发给主输出。
      yield element
    else:
      # 将此单词的长度发送到'above_cutoff_lengths'输出。
      yield pvalue.TaggedOutput(
          'above_cutoff_lengths', len(element))
    if element.startswith(marker):
      # 使用“marked strings”标签将此单词发送到不同的输出。
      yield pvalue.TaggedOutput('marked strings', element)

# 在Map和FlatMap中也可以生成多个输出。
# 下面是一个使用FlatMap的示例，它显示不需要预先指定标记。

def even_odd(x):
  yield pvalue.TaggedOutput('odd' if x % 2 else 'even', x)
  if x % 10 == 0:
    yield x

results = numbers | beam.FlatMap(even_odd).with_outputs()

evens = results.even
odds = results.odd
tens = results[None]  #未声明的主输出

```


#### 4.5.3. 在你DoFn中访问其他的参数 {#other-dofn-parameters}

除了元素和 `OutputReceiver`之外，Beam还会将其他参数填充到DoFn的 `@ProcessElement` 方法中。 可以按任何顺序将这些参数的任意组合添加到你的process方法中。


**时间戳:**
要访问输入元素的时间戳，请添加一个使用Instant类型的 `@Timestamp` 注解的参数。 例如： 

```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, @Timestamp Instant timestamp) {
  }})
```



**窗口:**
要访问输入元素所在的窗口，请添加用于输入 `PCollection` 的窗口类型的参数。 如果参数是与输入 `PCollection` 不匹配的窗口类型（ `BoundedWindow` 的子类），则会引发错误。 如果一个元素落在多个窗口中（例如，当使用 `SlidingWindows` 时会发生这种情况），那么将为该元素多次调用 `@ProcessElement` 方法，每个窗口调用一次。 例如，当使用固定窗口时，窗口的类型为  `IntervalWindow`。

```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, IntervalWindow window) {
  }})
```

**PaneInfo:**
当使用触发器时，Beam提供了一个 `PaneInfo` 对象，其中包含有关当前触发的信息。 使用  `PaneInfo`，您可以确定这是一个早还是晚触发，以及这个窗口已经为这个键触发了多少次。

```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PaneInfo paneInfo) {
  }})
```

**PipelineOptions:**
通过将它作为参数添加，始终可以在process方法中访问当前管道的 `PipelineOptions` ：
```java
.of(new DoFn<String, String>() {
     public void processElement(@Element String word, PipelineOptions options) {
  }})
```

`@OnTimer` 方法也可以访问这些参数。 可以在 `@OnTimer` 方法中访问Timestamp，window，`PipelineOptions`，`OutputReceiver` 和 `MultiOutputReceiver` 参数。 此外，`@OnTimer` 方法可以接受一个 `TimeDomain` 类型的参数，该参数指示计时器是基于事件时间还是处理时间。 定时器将在Apache Beam博客文章的[及时（和有状态）处理](https://beam.apache.org/blog/2017/08/28/timely-processing.html)中有更详细的解释。

### 4.6. 组合变换 {#composite-transforms}

变换可以具有嵌套结构，其中复杂变换执行多个更简单的变换（例如多个 `ParDo`，`Combine`，`GroupByKey` 或甚至其他组合变换）。 这些变换称为组合变换。 在单个组合变换中嵌套多个变换可以使您的代码更加模块化，更易于理解。

Beam SDK中包含许多有用的组合变换。 有关变换列表，请参阅API参考页面：
  * [为Java预先编写的Beam变换](https://beam.apache.org/releases/javadoc/2.13.0/index.html?org/apache/beam/sdk/transforms/package-summary.html)
  * [为Python预先编写的Beam变换](https://beam.apache.org/releases/pydoc/2.13.0/apache_beam.transforms.html)

#### 4.6.1. 一个组合变换的例子 {#composite-transform-example}

[WordCount示例程序](https://beam.apache.org/get-started/wordcount-example/)中的 `CountWords` 变换是组合变换的示例。`CountWords` 是一个 `PTransform` 子类，由多个嵌套变换组成。

在它的 `expand` 方法中，`CountWords` 变换应用以下变换操作：

  1. 它在文本行的输入 `PCollection` 上应用 `ParDo` ，产生单个单词的输出 `PCollection` 。
  2.它将Beam SDK库转换 `Count` 应用于单词的 `PCollection` 上，生成一个键/值对的 `PCollection` 。每个键代表文本中的一个单词，每个值表示该单词出现在原始数据中的次数。

请注意，这也是嵌套复合变换的一个例子，因为 `Count` 本身就是一个复合变换。

复合变换的参数和返回值必须与整个变换的初始输入类型和最终返回类型匹配，即使变换的中间数据多次更改类型也是如此。

```java
  public static class CountWords extends PTransform<PCollection<String>,
      PCollection<KV<String, Long>>> {
    @Override
    public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

      // 将文本行转换为单个单词。
      PCollection<String> words = lines.apply(
          ParDo.of(new ExtractWordsFn()));

      // 计算每个单词出现的次数。
      PCollection<KV<String, Long>> wordCounts =
          words.apply(Count.<String>perElement());

      return wordCounts;
    }
  }
```

```py
# WordCount管道内的CountWords复合变换。
class CountWords(beam.PTransform):

  def expand(self, pcoll):
    return (pcoll
            # 将文本行转换为单个单词。
            | 'ExtractWords' >> beam.ParDo(ExtractWordsFn())
            # 计算每个单词出现的次数。
            | beam.combiners.Count.PerElement()
            # 格式化每个单词并计入为一个可打印的字符串。
            | 'FormatCounts' >> beam.ParDo(FormatCountsFn()))

```

#### 4.6.2. 创建一个组合变换 {#composite-transform-creation}

要创建自己的组合变换，请创建 `PTransform` 类的子类并覆盖 `expand` 方法以指定实际的处理逻辑。 然后，您可以像使用Beam SDK中的内置变换一样使用此转换。

对于 `PTransform` 类类型参数，可以传递变换所用的 `PCollection` 类型作为输入，并生成输出。 要将多个 `PCollection` 作为输入，或生成多个 `PCollection` 作为输出，请使用多个集合类型之一作为相关类型参数。 

以下代码示例演示了如何声明一个 `PTransform`， 它接受一个作为 `String` 的 `PCollection` 输入，并输出一个 `Integer` 的 `PCollection` ：

```java
  static class ComputeWordLengths
    extends PTransform<PCollection<String>, PCollection<Integer>> {
    ...
  }
```

```Py
class ComputeWordLengths(beam.PTransform):
  def expand(self, pcoll):
    # 变换逻辑在这里
    return pcoll | beam.Map(lambda x: len(x))
```

在你的 `PTransform` 子类中，您需要覆盖 `expand` 方法。 您可以在 `expand` 方法中添加 `PTransform` 的处理逻辑。 您对 `expand` 的覆盖必须接受相应类型的输入 `PCollection` 作为参数，并将输出 `PCollection` 指定为返回值。

以下代码示例显示如何覆盖上一个示例中，在 `ComputeWordLengths` 类里面声明的的 `expand` 方法：

```java
  static class ComputeWordLengths
      extends PTransform<PCollection<String>, PCollection<Integer>> {
    @Override
    public PCollection<Integer> expand(PCollection<String>) {
      ...
      // 变换逻辑在这里
      ...
    }
```

```py
class ComputeWordLengths(beam.PTransform):
  def expand(self, pcoll):
    # 变换逻辑在这里
    return pcoll | beam.Map(lambda x: len(x))
```
只要你覆盖 `PTransform` 子类中的 `expand` 方法以接受适当的输入 `PCollection` 并返回相应的输出 `PCollection` ，你就可以包含任意数量的变换。 这些变换可以包括核心变换，组合变换或Beam SDK库中包含的变换。

**注意：**  `PTransform` 的 `expand` 方法不应由一个变换的用户直接调用。 相反，您应该在 `PCollection` 本身上调用 `apply` 方法，然后就将变换作为参数。 这允许变换嵌套在你的管道结构中。

#### 4.6.3. PTransform风格指南 {#ptransform-style-guide}

[PTransform风格指南](https://beam.apache.org/contribute/ptransform-style-guide/)包含这里没有包含的其他信息，例如风格指南，日志和测试指导以及特定于语言的注意事项。 当您想要编写新的组合PTransforms时，该指南是一个有用的起点。

## 5. 管道I/O. {#pipeline-io}

当你创建一个管道时，通常需要从某些外部源（例如文件或数据库）读取数据。 同样，您可能希望管道将它的结果数据输出到外部存储系统。 Beam为许多常见的数据存储类型提供读写变换。 如果希望管道读取或写入的数据存储格式是内置变换不支持的，则可以实现自己的读写变换。

### 5.1. 读取输入数据 {#pipeline-io-reading-data}

读取变换从外部源读取数据并返回数据的 `PCollection` 表示形式，以供管道使用。 虽然它在你的管道开始时最常见，但在构建管道时，你可以随时使用读取变换来创建一个新的 `PCollection`。

```java
PCollection<String> lines = p.apply(TextIO.read().from("gs://some/inputData.txt"));
```

```py
lines = pipeline | beam.io.ReadFromText('gs://some/inputData.txt')
```

### 5.2. 写入输出数据 {#pipeline-io-writing-data}

写变换将 `PCollection` 中的数据写到外部数据源。 你通常在管道的末尾使用写转换来输出管道的最终结果。 但是，您可以使用写变换在管道中的任何位置输出 `PCollection` 的数据。

```java
output.apply(TextIO.write().to("gs://some/outputData"));
```

```py
output | beam.io.WriteToText('gs://some/outputData')
```

### 5.3. 基于文件的输入和输出数据 {#file-based-data}

#### 5.3.1. 从多个位置读取 {#file-based-reading-multiple-locations}

许多读变换支持从您提供的一个glob运算符匹配的多个输入文件中读取。 请注意，glob运算符是特定于文件系统的，并遵循特定于文件系统的一致性模型。 以下TextIO示例使用一个glob运算符（*）来读取在给定位置具有前缀“input-”和后缀“.csv”的所有匹配输入文件：

```java
p.apply(“ReadFromText”,
    TextIO.read().from("protocol://my_bucket/path/to/input-*.csv");
```

```py
lines = p | 'ReadFromText' >> beam.io.ReadFromText('path/to/input-*.csv')
```
要将来自不同源的数据读入单个 `PCollection`，请单独读取每个 `PCollection`，然后使用 [Flatten](#flatten)变换创建单个 `PCollection`。

#### 5.3.2. 写入多个输出文件 {#file-based-writing-multiple-files}

对于基于文件的输出数据，默认情况下，写变换将写入多个输出文件。 当你将输出文件名传递给写变换时，文件名将用作写变换生成的所有输出文件的前缀。 您可以通过指定后缀为每个输出文件追加后缀。

以下的写变换示例将多个输出文件写入某个位置。 每个文件都有“numbers”前缀，一个数字标签和“.csv”后缀。

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

有关当前可用I/O变换的列表，请参见[Beam提供的I/O变换](https://beam.apache.org/documentation/io/built-in/)页面。

## 6. 数据编码和类型安全 {#data-encoding-and-type-safety}

当Beam运行程序执行你的管道时，它们通常需要在你的 `PCollection` 中物化中间数据，这需要将元素转换为字节字符串和从字节字符串转换回元素。 Beam SDK使用称为 `Coder` 的对象来描述如何编码和解码一个给定 `PCollection` 的元素。

>请注意，在与外部数据源或接收器交互时，编码器与解析或格式化数据无关。 通常应使用 `ParDo` 或 `MapElements` 等转换明确地进行此类解析或格式化。


在用于Java的Beam SDK中，类型 `Coder` 提供了编码和解码数据所需的方法。在用于Java的SDK中，提供了许多Coder子类，它们可以处理各种标准Java类型，例如Integer，Long，Double，StringUtf8等。 您可以在[Coder包](https://github.com/apache/beam/tree/master/sdks/java/core/src/main/java/org/apache/beam/sdk/coders)中找到所有可用的Coder子类。


在Python的Beam SDK中，`Coder` 类型提供了编码和解码数据所需的方法。在用于Python的SDK中，提供了许多Coder子类，它们可以处理各种标准Python类型，例如基本类型，Tuple，Iterable，StringUtf8等。 您可以在[apache_beam.coders](https://github.com/apache/beam/tree/master/sdks/python/apache_beam/coders)包中找到所有可用的Coder子类。

> 请注意，编码器与类型不一定具有1：1的关系。 例如，整数类型可以有多个有效的编码器，输入和输出数据可以使用不同的整数类型编码器。 一个变换可能包含使用BigEndianIntegerCoder的整数类型输入数据和使用VarIntCoder的整数类型输出数据。

### 6.1. 指定编码器 {#specifying-coders}

Beam SDKs需要为你的管道中的每个 `PCollection` 提供一个编码器。在大多数情况下，Beam SDK能够根据它的元素类型或产生它的变换自动推断出 `PCollection` 的 `Coder` ，但是，在某些情况下，管道作者需要明确指定一个 `Coder` ，或者为他们的自定义类型开发一个 `Coder` 。 

您可以使用 `PCollection.setCoder` 方法为现有的 `PCollection` 显式设置编码器。 请注意，您不能在已完成的 `PCollection` 上调用 `setCoder` （例如，在它的上面调用 `.apply` ）。

您可以使用 `getCoder` 方法获取现有 `PCollection` 的编码器。 如果尚未设置编码器且无法为给定的 `PCollection`推断出编码器，则此方法将失败并返回 `IllegalStateException` 异常。

当尝试自动推断出一个 `PCollection`的 `Coder` 时，Beam SDK使用各种机制。

每个管道对象都有一个 `CoderRegistry`。`CoderRegistry` 表示Java类型到默认编码器的映射，管道应该使用这些默认编码器来处理每种类型的 `PCollection`。

Python的Beam SDK也有一个 `CoderRegistry` ，它表示Python类型到默认编码器的映射，每种类型的 `PCollection` 都应该使用默认编码器。 

默认情况下，用于Java的Beam SDK会自动使用变换函数对象中的类型参数（如 `DoFn`）为 `PTransform` 生成的一个 `PCollection` 元素推断出 `Coder` 。 例如，在 `ParDo` 的情况下，一个 `DoFn<Integer, String>` 函数对象接受 `Integer` 类型的输入元素，并生成 `String` 类型的输出元素。 在这种情况下，Java SDK将自动推断输出  `PCollection<String>`的默认 `Coder` （在默认管道 `CoderRegistry`中，这是 `StringUtf8Coder`）。

默认情况下，Python的Beam SDK使用变换函数对象中的类型提示（如 `DoFn`）自动为输出 `PCollection` 的元素推断出 `Coder` 。 在 `ParDo` 的情况下，例如具有类型提示 `@beam.typehints.with_input_types(int)` 和`@beam.typehints.with_output_types(str)` 的 `DoFn` 接受int类型的输入元素并生成str类型的输出元素。 在这种情况下，Python的Beam SDK将自动推断输出 `PCollection` 的默认 `Coder`（在默认管道`CoderRegistry`中，这是`BytesCoder`）。

> 注意：如果你使用 `Create` 变换从内存数据中创建 `PCollection`，则不能依赖编码器推断和默认编码器。`Create` 不能访问它的参数的任何键入信息，如果参数列表包含其确切的运行时类没有注册默认编码器的值，则可能无法推断编码器。


使用 `Create` 时，确保拥有正确编码器的最简单方法是在应用 `Create` 变换时调用 `withCoder` 。

### 6.2. 默认编码器和CoderRegistry   {#default-coders-and-the-coderregistry}

每个管道对象都有一个 `CoderRegistry` 对象，它将语言类型映射到管道应该为这些类型使用的默认编码器。 您可以使用你自己的 `CoderRegistry` 来查找给定类型的默认编码器，或者为给定类型注册新的默认编码器。

`CoderRegistry` 包含使用针对JavaPython的Beam SDK创建的任何管道的编码器到标准JavaPython类型的默认映射。 下表显示了标准映射：

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


您可以使用 `CoderRegistry.getCoder` 方法来确定Java类型的默认编码器。 您可以使用方法 `Pipeline.getCoderRegistry` 访问给定管道的 `CoderRegistry` 。 这允许您基于每个管道确定（或设置）Java类型的默认编码器：即“对于这个管道，验证的整数值是使用`BigEndianIntegerCoder`编码的”。


您可以使用 `CoderRegistry.get_coder` 方法来确定Python类型的默认编码器。 您可以使用 `coders.registry` 访问 `CoderRegistry`。 这允许您确定（或设置）Python类型的默认编码器。

#### 6.2.2. 设置一个类型的默认编码器 {#setting-default-coder}

要为一个特定管道设置JavaPython类型的默认编码器，您需要获取并修改管道的 `CoderRegistry` 。 你使用方法`Pipeline.getCoderRegistry`，`coders.registry`获取 `CoderRegistry` 对象，然后使用方法`CoderRegistry.registerCoder`，`CoderRegistry.register_coder`为目标类型注册新的 `Coder`。

以下示例代码演示了如何为管道的Integerint值设置默认编码器（在本例中为`BigEndianIntegerCoder`）。

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

如果你的管道程序定义了自定义数据类型，你可以使用 `@DefaultCoder` 注解来指定要与该类型一起使用的编码器。 例如，假设您有一个要使用 `SerializableCoder` 的自定义数据类型。 您可以使用 `@DefaultCoder` 注解，如下所示：

```java
@DefaultCoder(AvroCoder.class)
public class MyCustomDataType {
  ...
}
```


如果您已创建了一个自定义编码器来匹配你的数据类型，并且您想使用 `@DefaultCoder` 注解，那么你的编码器类必须实现静态 `Coder.of(Class<T>)` 工厂方法。

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

Python的Beam SDK不支持使用默认编码器注解数据类型。 如果要设置默认编码器，请使用上一节*为一个类型设置默认编码器*中描述的方法。

## 7. 窗口 {#windowing}

窗口化根据它们的各个元素的时间戳细分一个 `PCollection` 。 变换聚合多个元素（例如 `GroupByKey` 和 `Combine`）在每个窗口的基础上隐式工作 - 它们将每个 `PCollection` 转化为一个接一个的多个有限窗口处理，尽管整个集合本身可能具有无限大小。

一个相关的概念，称为**触发器**，它确定何时在无界数据到达时发出聚合结果。你可以使用触发器来优化 `PCollection` 的窗口策略。 触发器允许你处理后期达到的数据或提供早期结果。 有关更多信息，请参阅[触发器](#triggers)部分。

### 7.1.窗口基础知识 {#windowing-basics}

一些Beam变换（例如 `GroupByKey` 和 `Combine`），通过一个公共键对多个元素进行分组。 通常，将整个数据集中具有相同键的所有元素进行分组操作。 利用无界数据集，不可能收集所有元素，因为新元素不断被添加并且可能无限多（例如流数据）。 如果您正在使用无界 `PCollection`，则窗口化尤其有用。

在Beam模型中，任何 `PCollection` （包括无界 `PCollection`）都可以细分为逻辑窗口。 根据 `PCollection` 的窗口函数将 `PCollection` 中的每个元素分配给一个或多个窗口，并且每个单独的窗口包含有限数量的元素。然后，对变换进行分组，在每个窗口的基础上考虑每个 `PCollection` 的元素。 例如，`GroupByKey` 通过键和窗口隐式地对 `PCollection`的元素进行分组。

**警告:** Beam的默认窗口行为是将 `PCollection` 的所有元素分配给单个全局窗口并丢弃后期数据，即使对于无界 `PCollection` 也是如此。 在无界 `PCollection` 上使用 `GroupByKey` 之类的分组转换之前，必须至少执行以下操作之一：
 
 * 设置一个非全局窗口函数。 请参阅[设置PCollection的窗口函数](#setting-your-pcollections-windowing-function)。
 * 设置一个非默认[触发器](#triggers)。 这允许全局窗口在其他条件下发出结果，因为默认的窗口行为（等待所有数据到达）将永远不会发生。

如果没有为无界 `PCollection` 设置非全局窗口函数或非默认触发器，并随后使用分组转换（如 `GroupByKey` 或 `Combine`），则管道将在构造时生成错误，您的作业将失败。

#### 7.1.1. 窗口约束 {#windowing-constraints}

在为 `PCollection` 设置窗口函数后，下次将分组变换应用于该 `PCollection` 时将使用元素的窗口。 根据需要对窗口进行分组。如果使用 `Window` 变换设置窗口函数，则会将每个元素分配给一个窗口，但只有在 `GroupByKey` 或 `Combine` 跨窗口和键聚合时才会考虑窗口。 这可能会对您的管道产生不同的影响。 考虑下图中的管道示例：

<img src="https://beam.apache.org/images/windowing-pipeline-unbounded.png" alt="应用窗口的管道图">

**图:** 管道应用窗口

在上面的管道中，我们通过使用 `KafkaIO` 读取一组键/值对来创建一个无界 `PCollection` ，然后使用 `Window` 变换将窗口函数应用于该集合。 然后，我们将 `ParDo` 应用于集合，然后使用 `GroupByKey` 对该 `ParDo` 的结果进行分组。 窗口函数对 `ParDo` 转换没有影响，因为在 `GroupByKey` 需要窗口之前，实际上并未使用窗口。 但是，后续变换将应用于 `GroupByKey` 的结果 - 数据按键和窗口分组。

#### 7.1.2. 使用有界PCollections进行窗口化 {#windowing-bounded-collections}

您可以在**有界** `PCollection` 中使用具有固定大小数据集的窗口。 但是，请注意，窗口仅考虑附加到 `PCollection` 的每个元素的隐式时间戳，并且创建固定数据集（例如 `TextIO`）的数据源为每个元素分配相同的时间戳。 这意味着默认情况下，所有元素都是单个全局窗口的一部分。

要使用具有固定数据集的窗口，你可以为每个元素分配自己的时间戳。 要为元素分配时间戳，请使用带有 `DoFn` 的 `ParDo` 变换，该变换使用新的时间戳输出每个元素（例如，用于Java的Beam SDK中的[WithTimestamps](https://beam.apache.org/releases/javadoc/2.13.0/index.html?org/apache/beam/sdk/transforms/WithTimestamps.html)变换）。

为了说明有界 `PCollection` 的窗口化如何影响管道处理数据的方式，请考虑以下管道：

<img src="https://beam.apache.org/images/unwindowed-pipeline-bounded.png" alt="在有界集合上没有窗口的GroupByKey和ParDo图">

**图:** 在一个有界集合上，`GroupByKey` 和 `ParDo` 没有窗口。

在上面的管道中，我们通过使用 `TextIO` 读取一组键/值对来创建一个有界 `PCollection` 。 然后，我们使用 `GroupByKey` 对集合进行分组，并将 `ParDo` 变换应用于分组的 `PCollection`。 在此示例中，`GroupByKey` 创建一组唯一键，然后 `ParDo` 为每个键只应用一次。

请注意，即使您没有设置窗口函数，仍然有一个窗口 - 你的 `PCollection` 中的所有元素都分配给一个全局窗口。

现在，考虑相同的管道，但使用窗口函数：

<img src="https://beam.apache.org/images/windowing-pipeline-bounded.png" alt="在一个有界集合上,具有窗口的GroupByKey和ParDo图">

**图:** 在一个有界集合上,具有窗口的 `GroupByKey` 和 `ParDo` 图

和前面一样，管道创建了一个键/值对的有界 `PCollection` 。 然后我们为该 `PCollection` 设置一个[窗口函数](#setting-your-pcollections-windowing-function)。  `GroupByKey` 变换基于窗口函数通过键和窗口对 `PCollection` 的元素进行分组。后续的 `ParDo` 变换为每个键应用多次，每个窗口一次。

### 7.2. 提供了窗口函数 {#provided-windowing-functions}

您可以定义不同类型的窗口来划分 `PCollection` 的元素。 Beam提供多种窗口功能，包括：
*  固定时间窗口
*  滑动时间窗口
*  每会话窗口
*  单一全局窗口
*  基于日历的窗口 (Python的Beam SDK不支持)

如果您有更复杂的需求，也可以定义自己的 `WindowFn`。

请注意，每个元素在逻辑上可以属于多个窗口，具体取决于你使用的窗口函数。 例如，滑动时间窗口创建重叠窗口，其中可以将单个元素分配给多个窗口。


#### 7.2.1. 固定时间窗口 {#fixed-time-windows}

最简单的窗口形式是使用**固定时间窗口**：给定一个带时间戳的 `PCollection` ，它可以不断更新，每个窗口可以捕获（例如）所有时间戳的元素，这些元素的时间间隔为5分钟。

固定时间窗口表示数据流中的一致持续时间，非重叠时间间隔。 考虑具有五分钟持续时间的窗口：时间戳值从0:00:00到0:05:00（但不包括）的无界 `PCollection` 中的所有元素都属于第一个窗口，元素的时间戳值为0 ：05：00到0:10:00（但不包括）属于第二个窗口，依此类推。

<img src="https://beam.apache.org/images/fixed-time-windows.png" alt="固定时间窗口图，持续时间为30秒">

**图:** 固定时间窗口图，持续时间为30秒

#### 7.2.2. 滑动时间窗口 {#sliding-time-windows}

一个**滑动时间窗口**还表示数据流中的时间间隔; 然而，滑动时间窗口可以重叠。 例如，每个窗口可能会捕获5分钟的数据，但每隔10秒就会启动一个新窗口。 滑动窗口开始的频率称为 _周期_。 因此，我们的示例将具有5分钟的窗口 _持续_ 时间和10秒的时间 _周期_。

由于多个窗口重叠，因此数据集中的大多数元素将属于多个窗口。 这种窗口对于获取数据运行平均值很有用; 使用滑动时间窗口，您可以计算过去5分钟数据的运行平均值，在我们的示例中每10秒更新一次。

<img src="https://beam.apache.org/images/sliding-time-windows.png" alt="滑动时间窗口示意图，窗口持续时间为1分钟，窗口周期为30秒">

**图:** 滑动时间窗口，窗口持续时间为1分钟，窗口周期为30秒。

#### 7.2.3. 会话窗口 {#session-windows}

一个**会话窗口**函数定义包含元素的窗口，这些元素在另一个元素的特定间隔持续时间内。 会话窗口适用于每个键的基础之上，对于随时间不规则分布的数据非常有用。 例如，表示用户鼠标活动的数据流可能有很长一段空闲时间，其间穿插着大量单击。如果数据在指定的最小间隔持续时间之后到达，则启动新窗口的开始。

<img src="https://beam.apache.org/images/session-windows.png" alt="具有最小间隙持续时间的会话窗口图">

**图:** 会话窗口，具有最小的间隔时间。注意，根据数据分布，每个数据键都有不同的窗口。

#### 7.2.4. 单一的全局窗口 {#single-global-window}

默认情况下， `PCollection` 中的所有数据都被分配给单个全局窗口，并且后期数据将被丢弃。 如果你的数据集具有固定大小，则可以对你的  `PCollection`  使用全局窗口默认值。

如果你使用的是无界数据集（例如，来自流数据源），则可以使用单个全局窗口，但在应用聚合变换（例如 `GroupByKey` 和 `Combine`）时要小心。 带有默认触发器的单个全局窗口通常要求在处理之前整个数据集是可用的，而连续更新数据是不可能做到这一点的。 要对使用全局窗口的无界 `PCollection` 执行聚合，您应该为该 `PCollection` 指定一个非默认触发器。

### 7.3. 设置你PCollection的窗口函数 {#setting-your-pcollections-windowing-function}

您可以通过应用 `Window` 变换为一个 `PCollection`设置窗口函数。 应用 `Window` 变换时，必须提供一个 `WindowFn`。  `WindowFn` 确定 `PCollection` 将用于后续分组变换的窗口函数，例如固定或滑动时间窗口。

当你设置窗口函数时，您可能还需要为 `PCollection` 设置一个触发器。 触发器确定何时聚合和发出每个单独的窗口，并且有助于改进窗口函数对于后期数据和计算早期结果的执行方式。 有关更多信息，请参阅[触发器](#triggers)部分。

#### 7.3.1. 固定时间窗口 {#using-fixed-time-windows}

以下示例代码显示如何应用 `Window` 将一个 `PCollection` 分成固定窗口，每个窗口的长度为60秒：

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

以下示例代码显示了如何应用 `Window` 将一个 `PCollection` 划分为滑动时间窗口。 每个窗口的长度为30秒，每5秒钟开始一个新窗口：

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

以下示例代码显示了如何应用 `Window` 将一个 `PCollection` 划分为会话窗口，其中每个会话必须以至少10分钟（600秒）的时间间隔分隔：

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

请注意，会话是每键一个 - 集合中的每个键都有自己的会话分组，具体取决于数据分布。


#### 7.3.4. 单一的全局窗口 {#using-single-global-window}

如果您的 `PCollection` 是有界（大小固定）的，您可以将所有元素分配到单个全局窗口。 以下示例代码显示如何为一个 `PCollection` 设置单个全局窗口：

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

在任何数据处理系统中，数据事件发生的时间（“事件时间”，由数据元素本身的时间戳确定）与你的管道中任何阶段处理实际数据元素的时间（“处理时间”，由处理元素的系统上的时钟决定）之间存在一定的延迟。 此外，不能保证数据事件将以生成它们的相同顺序出现在您的管道中。

例如，假设我们有一个使用固定时间窗口的 `PCollection` ，窗口长达5分钟。 对于每个窗口，Beam必须在给定的窗口范围内收集具有事件时间戳的所有数据（例如，在第一个窗口中的0:00和4:59之间）。 时间戳超出该范围的数据（来自5:00或更晚的数据）属于不同的窗口。

但是，并不总能保证数据按时间顺序到达管道，或始终以可预测的间隔到达。 Beam跟踪 _水印_，这是系统的概念，即即当某个窗口中的所有数据都可以预期到达管道时。 一旦 _水印_ 越过窗口的末尾，在该窗口中带有时间戳的任何其他元素被认为是**延迟数据**。

从我们的示例中，假设我们有一个简单的水印，假设数据时间戳（事件时间）和数据在管道中出现的时间（处理时间）之间的延迟时间约为30秒，那么Beam将在5：30关闭第一个窗口。 如果数据记录在5:34到达，但是时间戳会将它置于0：00-4：59窗口（例如，3：38），则该记录就是延迟数据。

注意：为简单起见，我们假设我们使用非常直观的水印来估计延迟时间。 实际上， `PCollection` 的数据源确定了水印，水印可以更精确或更复杂。

Beam的默认窗口配置尝试确定所有数据何时到达（基于数据源的类型），然后将水印推进到窗口的末尾。 此默认配置不允许延迟数据。 [触发器](#triggers)允许您修改和优化一个 `PCollection` 的窗口策略。 您可以使用触发器来确定每个单独窗口何时聚合并报告其结果，包括窗口如何发出后期元素。

#### 7.4.1. 管理后期数据 {#managing-late-data}

> **注意:** Python的Beam SDK不支持管理延迟数据。

当你在设置你的 `PCollection` 的窗口策略时，可以通过调用 `.withAllowedLateness` 操作来允许延迟数据。 下面的代码示例演示了一种窗口策略，该策略允许在窗口结束后最多两天的延迟数据。

```java
    PCollection<String> items = ...;
    PCollection<String> fixedWindowedItems = items.apply(
        Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
              .withAllowedLateness(Duration.standardDays(2)));
```
当您在一个 `PCollection` 上设置 `.withAllowedLateness` 时，允许延迟向前传播到从你应用的第一个允许延迟 `PCollection` 派生的任何后续 `PCollection`。 如果你要在稍后的管道中更改允许的延迟，则必须通过应用 `Window.configure().withAllowedLateness()` 来明确地这样做。

### 7.5. 将时间戳添加到一个PCollection的元素{#adding-timestamps-to-a-pcollections-elements}

无界数据源为每个元素提供一个时间戳。 根据您的无界数据源，您可能需要配置从原始数据流中提取时间戳的方式。

但是，有界数据源（例如 `TextIO` 中的文件）不提供时间戳。 如果需要时间戳，则必须将它们添加到你的 `PCollection` 的元素中。

你可以通过应用[ParDo](#pardo)变换为一个 `PCollection` 的元素分配新的时间戳，该变换输出带有你设置的时间戳的新元素。

例如，如果您的管道从输入文件中读取日志记录，并且每个日志记录都包含一个时间戳字段; 由于管道从文件中读取记录，因此文件源不会自动分配时间戳。 您可以解析每条记录中的时间戳字段，并使用带有 `DoFn` 的 `ParDo` 变换将时间戳附加到你 `PCollection` 中的每个元素。

```java
      PCollection<LogEntry> unstampedLogs = ...;
      PCollection<LogEntry> stampedLogs =
          unstampedLogs.apply(ParDo.of(new DoFn<LogEntry, LogEntry>() {
            public void processElement(@Element LogEntry element, OutputReceiver<LogEntry> out) {
              // 从我们当前正在处理的日志项中提取时间戳。
              Instant logTimeStamp = extractTimeStampFromLogEntry(element);
              // 使用OutputReceiver.outputWithTimestamp（而不是OutputReceiver.output）来发出附加了时间戳的条目。
              out.outputWithTimestamp(element, logTimeStamp);
            }
          }));
```
```py
class AddTimestampDoFn(beam.DoFn):

  def process(self, element):
    # 提取与当前日志项关联的Unix秒纪元以来的时间戳数字。
    unix_timestamp = extract_timestamp_from_log_entry(element)
    # 在TimestampedValue中包装并发出当前项和新时间戳。
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