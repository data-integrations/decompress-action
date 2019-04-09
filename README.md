# Decompress Action

![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)
![cdap-action](https://cdap-users.herokuapp.com/assets/cdap-action.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

Description
-----------
The Decompress Action is used to expand archived or compressed files before further processing in 
a pipeline. It relies on the [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/) library and
can handle many common formats including ``.zip``, ``.gz``, ``.tar``, ``.tar.gz`` and many more.

Use Case
--------
When pulling files from an external source, they may be packaged in an unsplittable format or you may want
to process files prior to running a pipeline. You can use this Action to expand those formats for processing.

Properties
----------
| Configuration | Required | Default | Description |
| :------------ | :------: | :------ | :---------- |
| **Source Path** | **Y** | None | The full path of the file or directory that is to be converted. In the case of a directory, if fileRegex is set, then only files in the source directory matching the regex expression will be moved. Otherwise, all files in the directory will be moved. For example: `hdfs://hostname/tmp`. You can use globbing syntax here. |
| **Destination Path** | **Y** | None | The full path where the file or files are to be saved. If a directory is specified the files will be created in that directory. If the Source Path is a directory, it is assumed that Destination Path is also a directory. Files with the same name will be overwritten. |
| **File Regular Expression** | **N** | None | Regular expression to filter the files in the source directory that will be moved. This is useful when the globbing syntax in the source directory is not precise enough for your files. |
| **Archived or Compressed?** | **Y** | Archived | Specify whether the files you are processing are archived (.zip, .tar), compressed (.gz, .bz2), or archived then compressed (.tar.gz, .tar.bz2). The intermediate archive is automatically deleted after successful processing. |
| **Continue Processing If There Are Errors?** | **Y** | false | Indicates if the pipeline should continue if processing the files fails. |

Build
-----
To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

UI Integration
--------------
The CDAP UI displays each plugin property as a simple textbox. To customize how the plugin properties
are displayed in the UI, you can place a configuration file in the ``widgets`` directory.
The file must be named following a convention of ``[plugin-name]-[plugin-type].json``.

See [Plugin Widget Configuration](http://docs.cdap.io/cdap/current/en/hydrator-manual/developing-plugins/packaging-plugins.html#plugin-widget-json)
for details on the configuration file.

The UI will also display a reference doc for your plugin if you place a file in the ``docs`` directory
that follows the convention of ``[plugin-name]-[plugin-type].md``.

When the build runs, it will scan the ``widgets`` and ``docs`` directories in order to build an appropriately
formatted .json file under the ``target`` directory. This file is deployed along with your .jar file to add your
plugins to CDAP.

Deployment
----------
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/plugin.jar> config-file <target/plugin.json>

For example, here if your artifact is named 'azure-decompress-action-1.0.0.jar':

    > load artifact target/azure-decompress-action-1.0.0.jar config-file target/azure-decompress-action-1.0.0.json

## Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

## IRC Channel

CDAP IRC Channel: #cdap on irc.freenode.net


## License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
