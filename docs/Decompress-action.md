# Decompress Action


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

Usage Notes
-----------

This plugin relies [Apache Commons Compress](http://commons.apache.org/proper/commons-compress/) and there are some features of archived files
which are not supported. This plugin will throw a warning if there are entities in the archive that are using features that are not supported.

When specifying an archive to expand, the destination should be a folder. When specifying a single or multiple archives, each one will be expanded
into its own subfolder in the destination based on the archive name. For example, if the dest is ``/tmp/`` and the archive is ``example.zip``, the files will be available in ``/tmp/example/file1``.