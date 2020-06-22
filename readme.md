# Using EnOS Data Subscription SDK for .NET

Table of Contents

* [Installation](#install)
    * [Prerequisites](#pre)
    * [Installing from Package Manager Console](#pmc)
    * [Building From Source](#obtain)
* [Feature List](#feature)
* [Sample Codes](#sample)
* [Related Information](#information)
* [Release Notes](#releasenotes)


EnOS Data Subscription Service improves the API calling efficiency of applications with active data push, which supports subscription to real-time asset data, offline asset data, and asset alert data.

After configuring and starting data subscription jobs on the EnOS Management Console, you can use the Data Subscription SDK for .NET to develop applications for consuming the subscribed data.


<a name="install"></a>

## Installation

<a name="pre"></a>

### Prerequisites

The Data Subscription SDK is for .NET Framework 4.7, and newer versions.


You can install the SDK from Package Manager Console, or build from source.

<a name="pmc"></a>

### Installing from Package Manager Console

The latest version of EnOS Data Subscription SDK for .NET is available in the Package Manager Console and can be installed using:

```
Install-Package enos-subscribe
```

<a name="obtain"></a>

### Building From Source

1. Obtain the source code of Data Subscription SDK for .NET.
   - From GitHub:
    ```
    git clone https://github.com/EnvisionIot/enos-subscription-service-sdk-dotnet.git
    ```
   - From EnOS SDK Center. Click **SDK Center** from the left navigation of EnOS Console, and obtain the SDK source code by clicking the GitHub icon in the **Obtain** column.


2. In Visual Studio, add the source code project into your solution and add as reference to your project.


The EnOS Data Subscription SDK for .NET has the following dependency modules:
- [protobuf-net](https://github.com/protobuf-net/protobuf-net)
- [NLog](https://github.com/NLog/NLog)


<a name="feature"></a>

## Feature List

EnOS Enterprise Data Platform supports subscribing to asset time series data and alert data and pushing the subscribed data to applications, thus improving the data query efficiency of applications.

The features supported by this SDK include:
- Consuming subscribed real-time asset data
- Consuming subscribed alert data
- Consuming subscribed offline asset data


<a name="sample"></a>

## Sample Codes

### Code Sample for Consuming Subscribed Real-time Data

```csharp
using enos_subscription.client;

using (DataClient client = new DataClient("sub-host", "sub-port", "Your Access Key of this subscription", "Your Access Secret of this subscription")) {    
    
    client.subscribe("Your subscription Id", "Your consumer group");

    foreach (var message in client.GetMessages())
    {
       //do something with the message
    }
}
```

### Code Sample for Consuming Subscribed Alert Data

```csharp
using enos_subscription.client;

using (AlertClient client = new AlertClient("sub-host", "sub-port", "Your Access Key of this subscription", "Your Access Secret of this subscription")) {    
    
    client.subscribe("Your subscription Id", "Your consumer group");

    foreach (var message in client.GetMessages())
    {
       //do something with the message
    }
}
```

### Code Sample for Consuming Subscribed Offline Data

```csharp
using enos_subscription.client;

using (OfflineClient client = new OfflineClient("sub-host", "sub-port", "Your Access Key of this subscription", "Your Access Secret of this subscription")) {    
    
    client.subscribe("Your subscription Id", "Your consumer group");

    foreach (var message in client.GetMessages())
    {
       //do something with the message
    }
}
```


<a name="information"></a>

## Related Information

To learn more about the Data Subscription feature of EnOS Enterprise Data Platform, see [Data Subscription Overview](https://support.envisioniot.com/docs/data-asset/en/latest/learn/data_subscription_overview.html).


<a name="releasenotes"></a>

## Release Notes

- 2020/04/24 (2.4.1): Initial release
