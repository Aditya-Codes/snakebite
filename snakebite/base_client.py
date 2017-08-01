import snakebite.protobuf.ClientNamenodeProtocol_pb2 as client_proto
import snakebite.glob as glob
from snakebite.platformutils import get_current_username
from snakebite.channel import DataXceiverChannel
from snakebite.config import HDFSConfig
from snakebite.errors import (
    ConnectionFailureException,
    DirectoryException,
    FileAlreadyExistsException,
    FileException,
    FileNotFoundException,
    InvalidInputException,
    OutOfNNException,
    RequestError,
    FatalException, TransientException)
from snakebite.namenode import Namenode
from snakebite.service import RpcService

import Queue
import zlib
import bz2
import logging
import os
import os.path
import posixpath
import fnmatch
import inspect
import socket
import errno
import time
import re
import sys
import random

log = logging.getLogger(__name__)


class BaseClient(object):
    def __init__(self,
                 hadoop_version=Namenode.DEFAULT_VERSION,
                 use_trash=False,
                 use_sasl=False,
                 hdfs_namenode_principal=None,
                 use_datanode_hostname=False):
        """
        :param hadoop_version: What hadoop protocol version should be used (default: 9)
        :type hadoop_version: int
        :param use_trash: Use a trash when removing files.
        :type use_trash: boolean
        :param effective_user: Effective user for the HDFS operations (default: None - current user)
        :type effective_user: string
        :param use_sasl: Use SASL authentication or not
        :type use_sasl: boolean
        :param hdfs_namenode_principal: Kerberos principal to use for HDFS
        :type hdfs_namenode_principal: string
        :param sock_connect_timeout: Socket connection timeout in seconds
        :type sock_connect_timeout: int
        :param sock_request_timeout: Request timeout in seconds
        :type sock_request_timeout: int
        :param use_datanode_hostname: Use hostname instead of IP address to commuicate with datanodes
        :type use_datanode_hostname: boolean
        """
        if hadoop_version < 9:
            raise FatalException("Only protocol versions >= 9 supported")

        self.use_sasl = use_sasl
        self.hdfs_namenode_principal = hdfs_namenode_principal
        self.use_trash = use_trash
        self.trash = self._join_user_path(".Trash")
        self._server_defaults = None
        self.use_datanode_hostname = use_datanode_hostname

        # log.debug("Created client for %s:%s with trash=%s and sasl=%s" % (host, port, use_trash, use_sasl))

    FILETYPES = {
        1: "d",
        2: "f",
        3: "s"
    }

    LISTING_ATTRIBUTES = ['length', 'owner', 'group', 'block_replication',
                          'modification_time', 'access_time', 'blocksize']

    COUNT_ATTRIBUTES = ['length', 'fileCount', 'directoryCount', 'quota', 'spaceConsumed', 'spaceQuota']

    def ls(self, paths, recurse=False, include_toplevel=False, include_children=True):
        """ Issues 'ls' command and returns a list of maps that contain fileinfo

        :param paths: Paths to list
        :type paths: list
        :param recurse: Recursive listing
        :type recurse: boolean
        :param include_toplevel: Include the given path in the listing. If the path is a file, include_toplevel is always True.
        :type include_toplevel: boolean
        :param include_children: Include child nodes in the listing.
        :type include_children: boolean
        :returns: a generator that yields dictionaries

        **Examples:**

        Directory listing

        >>> list(client.ls(["/"]))
        [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1367317324982L, 'block_replication': 1, 'modification_time': 1367317325346L, 'length': 6783L, 'blocksize': 134217728L, 'owner': u'wouter', 'path': '/Makefile'}, {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0L, 'block_replication': 0, 'modification_time': 1367317325431L, 'length': 0L, 'blocksize': 0L, 'owner': u'wouter', 'path': '/build'}, {'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1367317326510L, 'block_replication': 1, 'modification_time': 1367317326522L, 'length': 100L, 'blocksize': 134217728L, 'owner': u'wouter', 'path': '/index.asciidoc'}, {'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0L, 'block_replication': 0, 'modification_time': 1367317326628L, 'length': 0L, 'blocksize': 0L, 'owner': u'wouter', 'path': '/source'}]

        File listing

        >>> list(client.ls(["/Makefile"]))
        [{'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'access_time': 1367317324982L, 'block_replication': 1, 'modification_time': 1367317325346L, 'length': 6783L, 'blocksize': 134217728L, 'owner': u'wouter', 'path': '/Makefile'}]

        Get directory information

        >>> list(client.ls(["/source"], include_toplevel=True, include_children=False))
        [{'group': u'supergroup', 'permission': 493, 'file_type': 'd', 'access_time': 0L, 'block_replication': 0, 'modification_time': 1367317326628L, 'length': 0L, 'blocksize': 0L, 'owner': u'wouter', 'path': '/source'}]
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")

        def _callback(path, node):
            """ Handle every node received for an ls request"""
            entry = {
                "file_type": self.FILETYPES[node.fileType],
                "permission": node.permission.perm,
                "path": path
            }

            for attribute in self.LISTING_ATTRIBUTES:
                entry[attribute] = node.__getattribute__(attribute)

            return entry

        for item in self._find_items(paths, _callback,
                                     include_toplevel=include_toplevel,
                                     include_children=include_children,
                                     recurse=recurse):
            if item:
                yield item

    def chmod(self, paths, mode, recurse=False):
        """ Change the mode for paths. This returns a list of maps containing the resut of the operation.

        :param paths: List of paths to chmod
        :type paths: list
        :param mode: Octal mode (e.g. 0o755)
        :type mode: int
        :param recurse: Recursive chmod
        :type recurse: boolean
        :returns: a generator that yields dictionaries

        .. note:: The top level directory is always included when `recurse=True`
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("chmod: no path given")
        if not mode:
            raise InvalidInputException("chmod: no mode given")

        service = self._get_service()

        def _callback(path, node):
            request = client_proto.SetPermissionRequestProto()
            request.src = path
            request.permission.perm = mode
            service.setPermission(request)

            return {"result": True, "path": path}

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=recurse):
            if item:
                yield item

    def __change_owner_or_group(self, path, owner=None, group=None):
        request = client_proto.SetOwnerRequestProto()
        request.src = path
        if owner:
            request.username = owner
        if group:
            request.groupname = group

        service = self._get_service()
        service.setOwner(request)
        return {"result": True, "path": path}

    def chown(self, paths, owner, recurse=False):
        """ Change the owner for paths. The owner can be specified as `user` or `user:group`

        :param paths: List of paths to chmod
        :type paths: list
        :param owner: New owner
        :type owner: string
        :param recurse: Recursive chown
        :type recurse: boolean
        :returns: a generator that yields dictionaries

        This always include the toplevel when recursing.
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("chown: no path given")
        if not owner:
            raise InvalidInputException("chown: no owner given")

        group = None
        if ":" in owner:
            (owner, group) = owner.split(':')

        def _callback(path, node):
            return self.__change_owner_or_group(path, owner, group)

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=recurse):
            if item:
                yield item

    def chgrp(self, paths, group, recurse=False):
        """ Change the group of paths.

        :param paths: List of paths to chgrp
        :type paths: list
        :param group: New group
        :type mode: string
        :param recurse: Recursive chgrp
        :type recurse: boolean
        :returns: a generator that yields dictionaries

        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("chgrp: no paths given")
        if not group:
            raise InvalidInputException("chgrp: no group given")

        def _callback(path, node):
            return self.__change_owner_or_group(path, group=group)

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=recurse):
            if item:
                yield item

    def count(self, paths):
        """ Count files in a path

        :param paths: List of paths to count
        :type paths: list
        :returns: a generator that yields dictionaries

        **Examples:**

        >>> list(client.count(['/']))
        [{'spaceConsumed': 260185L, 'quota': 2147483647L, 'spaceQuota': 18446744073709551615L, 'length': 260185L, 'directoryCount': 9L, 'path': '/', 'fileCount': 34L}]

        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("count: no path given")

        def _callback(path, node):
            request = client_proto.GetContentSummaryRequestProto()
            request.path = path

            service = self._get_service()
            response = service.getContentSummary(request)
            entry = {"path": path}
            for attribute in self.COUNT_ATTRIBUTES:
                entry[attribute] = response.summary.__getattribute__(attribute)
            return entry

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=False):
            if item:
                yield item

    def df(self):
        """ Get FS information

        :returns: a dictionary

        **Examples:**

        >>> client.df()
        {'used': 491520L, 'capacity': 120137519104L, 'under_replicated': 0L, 'missing_blocks': 0L, 'filesystem': 'hdfs://localhost:8020', 'remaining': 19669295104L, 'corrupt_blocks': 0L}
        """

        def _callback(path, node):
            service = self._get_service()
            request = client_proto.GetFsStatusRequestProto()
            response = service.getFsStats(request)
            entry = {
                "filesystem": "hdfs://%s:%d" % (self.host, self.port)
            }
            for i in ['capacity', 'used', 'remaining', 'under_replicated',
                      'corrupt_blocks', 'missing_blocks']:
                entry[i] = response.__getattribute__(i)

            return entry

        return list(self._find_items(['/'], _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=False))[0]

    def du(self, paths, include_toplevel=False, include_children=True):
        """Returns size information for paths

        :param paths: Paths to du
        :type paths: list
        :param include_toplevel: Include the given path in the result. If the path is a file, include_toplevel is always True.
        :type include_toplevel: boolean
        :param include_children: Include child nodes in the result.
        :type include_children: boolean
        :returns: a generator that yields dictionaries

        **Examples:**

        Children:

        >>> list(client.du(['/']))
        [{'path': '/Makefile', 'length': 6783L}, {'path': '/build', 'length': 244778L}, {'path': '/index.asciidoc', 'length': 100L}, {'path': '/source', 'length': 8524L}]

        Directory only:

        >>> list(client.du(['/'], include_toplevel=True, include_children=False))
        [{'path': '/', 'length': 260185L}]

        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("du: no path given")

        def _callback(path, node):
            if self._is_dir(node):
                request = client_proto.GetContentSummaryRequestProto()
                request.path = path
                try:
                    service = self._get_service()
                    response = service.getContentSummary(request)
                    return {"path": path, "length": response.summary.length}
                except RequestError as e:
                    print(e)
            else:
                return {"path": path, "length": node.length}

        for item in self._find_items(paths, _callback,
                                     include_toplevel=include_toplevel,
                                     include_children=include_children,
                                     recurse=False):
            if item:
                yield item

    def rename(self, paths, dst):
        """ Rename (move) path(s) to a destination

        :param paths: Source paths
        :type paths: list
        :param dst: destination
        :type dst: string
        :returns: a generator that yields dictionaries
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("rename: no path given")
        if not dst:
            raise InvalidInputException("rename: no destination given")

        def _callback(path, node):
            return self._handle_rename(path, node, dst)

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True):
            if item:
                yield item

    def _handle_rename(self, path, node, destination):
        # TODO Do we need this anymore once full path expansion works?
        if not destination.startswith("/"):
            destination = self._join_user_path(destination)

        destination = self._normalize_path(destination)

        request = client_proto.RenameRequestProto()
        request.src = path
        request.dst = destination
        service = self._get_service()
        response = service.rename(request)

        return {"path": path, "result": response.result}

    def rename2(self, path, dst, overwriteDest=False):
        """ Rename (but don't move) path to a destination

        By only renaming, we mean that you can't move a file or folder out or
        in other folder. The renaming can only happen within the folder the
        file or folder lies in.

        Note that this operation "always succeeds" unless an exception is
        raised, hence, the dict returned from this function  doesn't have the
        'result' key.

        Since you can't move with this operation, and only rename, it would
        not make sense to pass multiple paths to rename to a single
        destination. This method uses the underlying rename2 method.

        https://github.com/apache/hadoop/blob/ae91b13/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/protocol/ClientProtocol.java#L483-L523

        Out of all the different exceptions mentioned in the link above, this
        method only wraps the FileAlreadyExistsException exception.  You will
        also get a FileAlreadyExistsException if you have overwriteDest=True
        and the destination folder is not empty. The other exceptions will just
        be passed along.

        :param path: Source path
        :type path: string
        :param dst: destination
        :type dst: string
        :param overwriteDest: If the destination exists, overwrite it
        :type overwriteDest: boolean
        :returns: A dictionary or None
        """
        if not path:
            raise InvalidInputException("rename2: no path given")
        if not dst:
            raise InvalidInputException("rename2: no destination given")
        if not isinstance(path, (str, unicode)):
            raise InvalidInputException("rename2: Path should be a string")

        def _callback(path, node):
            destination = dst
            if not destination.startswith("/"):
                destination = self._join_user_path(destination)

            # Strip the last / if there is one. Hadoop doesn't like this
            if destination.endswith("/"):
                destination = destination[:-1]

            request = client_proto.Rename2RequestProto()
            request.src = path
            request.dst = destination
            request.overwriteDest = overwriteDest
            try:
                service = self._get_service()
                service.rename2(request)
            except RequestError as ex:
                if ("FileAlreadyExistsException" in str(ex) or
                            "rename destination directory is not empty" in str(ex)):
                    raise FileAlreadyExistsException(ex)
                else:
                    raise

            return {"path": path}

        for item in self._find_items([path], _callback, include_toplevel=True):
            return item

    def delete(self, paths, recurse=False):
        """ Delete paths

        :param paths: Paths to delete
        :type paths: list
        :param recurse: Recursive delete (use with care!)
        :type recurse: boolean
        :returns: a generator that yields dictionaries

        .. note:: Recursive deletion uses the NameNode recursive deletion functionality
                 instead of letting the client recurse. Hadoops client recurses
                 by itself and thus showing all files and directories that are
                 deleted. Snakebite doesn't.
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("delete: no path given")

        def _callback(path, node):
            return self._handle_delete(path, node, recurse)

        for item in self._find_items(paths, _callback, include_toplevel=True):
            if item:
                yield item

    def _handle_delete(self, path, node, recurse=False):
        if self._is_dir(node) and not recurse:
            raise DirectoryException("rm: `%s': Is a directory" % path)

        if not recurse:
            recurse = False

        if self.__should_move_to_trash(path):
            if path.endswith("/"):
                suffix_path = path[1:-1]
            else:
                suffix_path = path[1:]

            trash_path = posixpath.join(self.trash, "Current", suffix_path)
            if trash_path.endswith("/"):
                trash_path = trash_path[:-1]

            base_trash_path = posixpath.join(self.trash, "Current", posixpath.dirname(suffix_path))
            if base_trash_path.endswith("/"):
                base_trash_path = base_trash_path[:-1]

            # Try twice, in case checkpoint between mkdir() and rename()
            for i in range(0, 2):
                list(self.mkdir([base_trash_path], create_parent=True, mode=0o700))

                original_path = trash_path

                while self.test(trash_path, exists=True):
                    unix_timestamp = str(int(time.time() * 1000))
                    trash_path = "%s%s" % (original_path, unix_timestamp)

                result = self._handle_rename(path, node, trash_path)
                if result['result']:
                    result['message'] = ". Moved %s to %s" % (path, trash_path)
                    return result
            raise FatalException("Failed to move to trash: %s" % path)
        else:
            request = client_proto.DeleteRequestProto()
            request.src = path
            request.recursive = recurse
            service = self._get_service()
            response = service.delete(request)
            return {"path": path, "result": response.result}

    def __should_move_to_trash(self, path):
        if not self.use_trash:
            return False
        if path.startswith(self.trash):
            return False  # Path already in trash
        if posixpath.dirname(self.trash).startswith(path):
            raise FatalException("Cannot move %s to the trash, as it contains the trash" % path)

        return True

    def rmdir(self, paths):
        """ Delete a directory

        :param paths: Paths to delete
        :type paths: list
        :returns: a generator that yields dictionaries

        .. note: directories have to be empty.
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("rmdir: no path given")

        def _callback(path, node):
            return self._handle_rmdir(path, node)

        for item in self._find_items(paths, _callback, include_toplevel=True):
            if item:
                yield item

    def _handle_rmdir(self, path, node):
        if not self._is_dir(node):
            raise DirectoryException("rmdir: `%s': Is not a directory" % path)

        # Check if the directory is empty
        files = self.ls([path])
        if len(list(files)) > 0:
            raise DirectoryException("rmdir: `%s': Directory is not empty" % path)

        return self._handle_delete(path, node, recurse=True)

    def touchz(self, paths, replication=None, blocksize=None):
        """ Create a zero length file or updates the timestamp on a zero length file

        :param paths: Paths
        :type paths: list
        :param replication: Replication factor
        :param blocksize: Block size (in bytes) of the newly created file
        :type blocksize: int
        :returns: a generator that yields dictionaries
        """

        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("touchz: no path given")

        # Let's get the blocksize and replication from the server defaults
        # provided by the namenode if they are not specified
        if not replication or not blocksize:
            # TODO Add server defaults
            defaults = self.serverdefaults()

            if not replication:
                replication = defaults['replication']
            if not blocksize:
                blocksize = defaults['blockSize']

        def _callback(path, node):
            # Item already exists
            if node:
                if node.length != 0:
                    raise FileException("touchz: `%s': Not a zero-length file" % path)
                if self._is_dir(node):
                    raise DirectoryException("touchz: `%s': Is a directory" % path)

                response = self._create_file(path, replication, blocksize, overwrite=True)
            else:
                # Check if the parent directory exists
                parent = self._get_file_info(posixpath.dirname(path))
                if not parent:
                    raise DirectoryException("touchz: `%s': No such file or directory" % path)
                else:
                    response = self._create_file(path, replication, blocksize, overwrite=False)
            return {"path": path, "result": response.result}

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     check_nonexistence=True,
                                     include_children=False):
            if item:
                yield item

    def _create_file(self, path, replication, blocksize, overwrite):
        if overwrite:
            create_flag = 0x02
        else:
            create_flag = 0x01

        # Issue a CreateRequestProto
        request = client_proto.CreateRequestProto()
        request.src = path
        request.masked.perm = 0o644
        request.clientName = "snakebite"
        request.createFlag = create_flag
        request.createParent = False
        request.replication = replication
        request.blockSize = blocksize

        # The response doesn't contain anything
        service = self._get_service()
        service.create(request)

        # Issue a CompleteRequestProto
        request = client_proto.CompleteRequestProto()
        request.src = path
        request.clientName = "snakebite"

        return service.complete(request)

    def setrep(self, paths, replication, recurse=False):
        """ Set the replication factor for paths

        :param paths: Paths
        :type paths: list
        :param replication: Replication factor
        :type recurse: int
        :param recurse: Apply replication factor recursive
        :type recurse: boolean
        :returns: a generator that yields dictionaries
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("setrep: no path given")
        if not replication:
            raise InvalidInputException("setrep: no replication given")

        def _callback(path, node):
            if not self._is_dir(node):
                request = client_proto.SetReplicationRequestProto()
                request.src = path
                request.replication = replication
                service = self._get_service()
                response = service.setReplication(request)
                return {"result": response.result, "path": path}

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=recurse):
            if item:
                yield item

    def cat(self, paths, check_crc=False):
        """ Fetch all files that match the source file pattern
        and display their content on stdout.

        :param paths: Paths to display
        :type paths: list of strings
        :param check_crc: Check for checksum errors
        :type check_crc: boolean
        :returns: a generator that yields strings
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("cat: no path given")

        def _callback(path, node):
            if self._is_dir(node):
                raise DirectoryException("cat: `%s': Is a directory" % path)

            for load in self._read_file(path, node, False, check_crc):
                if load:
                    yield load

        for item in self._find_items(paths, _callback,
                                     include_toplevel=True,
                                     include_children=False,
                                     recurse=False):
            if item:
                yield item

    def copyToLocal(self, paths, dst, check_crc=False):
        """ Copy files that match the file source pattern
        to the local name.  Source is kept.  When copying multiple,
        files, the destination must be a directory.

        :param paths: Paths to copy
        :type paths: list of strings
        :param dst: Destination path
        :type dst: string
        :param check_crc: Check for checksum errors
        :type check_crc: boolean
        :returns: a generator that yields strings
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("copyToLocal: no path given")
        if not dst:
            raise InvalidInputException("copyToLocal: no destination given")

        dst = self._normalize_path(dst)

        def _callback(path, node):
            return self._handle_copyToLocal(path, node, dst, check_crc)

        for path in paths:
            # TODO Fix this base source
            self.base_source = None
            for item in self._find_items([path], _callback, include_toplevel=True, recurse=True, include_children=True):
                if item:
                    yield item

    def _handle_copyToLocal(self, path, node, dst, check_crc):
        # _handle_copyToLocal makes sense only per on dir tree
        # with common base_source
        dst = os.path.abspath(dst)
        if not self.base_source:
            # base_source is shared for whole dir tree, and can
            # be computed only once per dir tree
            self.base_source = posixpath.dirname(path)
            self.base_source = self.base_source if self.base_source.endswith("/") else self.base_source + "/"

        # If input destination is an existing directory, include toplevel
        if os.path.isdir(dst):
            local_prefix = path.replace(self.base_source, "", 1)
            target = os.path.join(dst, local_prefix)
        else:
            target = dst

        error = ""
        result = False
        # Target is an existing file
        if os.path.isfile(target):
            error += "file exists"
        # Target is an existing directory
        elif os.path.isdir(target):
            error += "directory exists"
        # Source is a directory
        elif self._is_dir(node):
            os.makedirs(target, mode=node.permission.perm)
            result = True
        # Source is a file
        elif self._is_file(node):
            temporary_target = "%s._COPYING_" % target
            f = open(temporary_target, 'wb')
            try:
                for load in self._read_file(path, node, tail_only=False, check_crc=check_crc):
                    f.write(load)
                f.close()
                os.rename(temporary_target, target)
                result = True
            except Exception as e:
                result = False
                error = e
                if os.path.isfile(temporary_target):
                    os.remove(temporary_target)

        return {"path": target, "result": result, "error": error, "source_path": path}

    def getmerge(self, path, dst, newline=False, check_crc=False):
        """ Get all the files in the directories that
        match the source file pattern and merge and sort them to only
        one file on local fs.

        :param path: Directory containing files that will be merged
        :type path: string
        :param dst: Path of file that will be written
        :type dst: string
        :param newline: Add a newline character at the end of each file.
        :type newline: boolean
        :returns: string content of the merged file at dst
        """
        if not path:
            raise InvalidInputException("getmerge: no path given")
        if not dst:
            raise InvalidInputException("getmerge: no destination given")

        temporary_target = "%s._COPYING_" % dst
        f = open(temporary_target, 'w')

        def _callback(_path, node):
            return self._handle_getmerge(_path, node, dst, check_crc)

        try:
            for item in self._find_items([path], _callback,
                                         include_toplevel=True,
                                         recurse=False,
                                         include_children=True):
                bytes_written_count = 0
                for load in item:
                    if load['result']:
                        f.write(load['response'])
                        bytes_written_count += len(load['response'])
                    elif load['error'] is not '':
                        if os.path.isfile(temporary_target):
                            os.remove(temporary_target)
                        raise FatalException(load['error'])

                if newline and bytes_written_count > 0:
                    f.write("\n")

            yield self._merge_result(dst, source_path=path)

        finally:
            if os.path.isfile(temporary_target):
                f.close()
                os.rename(temporary_target, dst)

    def _handle_getmerge(self, path, node, dst, check_crc):
        log.debug("in handle getmerge")

        dst = self._normalize_path(dst)

        if not self._is_file(node):
            # Target is an existing file
            if os.path.isfile(dst):
                yield self._merge_result(path, result=False, error='target file exists')
            # Target is an existing directory
            elif os.path.isdir(dst):
                yield self._merge_result(path, result=False, error='target directory exists')
        # Source is a file
        else:
            if node.length == 0:  # Empty file
                yield self._merge_result(path)
            else:
                try:
                    for file_contents in self._read_file(path, node, tail_only=False, check_crc=check_crc):
                        yield self._merge_result(path, response=file_contents)
                except Exception as e:
                    yield self._merge_result(path, result=False, error=str(e))

    @staticmethod
    def _merge_result(path, source_path=None, result=True, error='', response=''):
        if source_path is None:
            source_path = path

        return {
            "path": path,
            "response": response,
            "result": result,
            "error": error,
            "source_path": source_path
        }

    def stat(self, paths):
        """ Stat a fileCount

        :param paths: Path
        :type paths: string
        :returns: a dictionary

        **Example:**

        >>> client.stat(['/index.asciidoc'])
        {'blocksize': 134217728L, 'owner': u'wouter', 'length': 100L, 'access_time': 1367317326510L, 'group': u'supergroup', 'permission': 420, 'file_type': 'f', 'path': '/index.asciidoc', 'modification_time': 1367317326522L, 'block_replication': 1}
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("stat: no path given")

        def _callback(path, node):
            return {
                "path": path,
                "file_type": self.FILETYPES[node.fileType],
                "length": node.length,
                "permission": node.permission.perm,
                "owner": node.owner,
                "group": node.group,
                "modification_time": node.modification_time,
                "access_time": node.access_time,
                "block_replication": node.block_replication,
                "blocksize": node.blocksize
            }

        return list(self._find_items(paths, _callback, include_toplevel=True))[0]

    def tail(self, path, tail_length=1024, append=False):
        # Note: append is currently not implemented.
        """ Show the end of the file - default 1KB, supports up to the Hadoop block size.

        :param path: Path to read
        :type path: string
        :param tail_length: The length to read from the end of the file - default 1KB, up to block size.
        :type tail_length: int
        :param append: Currently not implemented
        :type append: bool
        :returns: a generator that yields strings
        """
        # TODO: Make tail support multiple files at a time, like most other methods do

        if not path:
            raise InvalidInputException("tail: no path given")

        block_size = self.serverdefaults()['blockSize']
        if tail_length > block_size:
            raise InvalidInputException("tail: currently supports length up to the block size (%d)" % (block_size,))

        if tail_length <= 0:
            raise InvalidInputException("tail: tail_length cannot be less than or equal to zero")

        def _callback(_path, node):
            data = ''
            for load in self._read_file(_path, node, tail_only=True, check_crc=False, tail_length=tail_length):
                data += load

            # We read only the necessary packets but still
            # need to cut off at the packet level.
            cutoff_beginning = max(0, len(data) - tail_length)
            return data[cutoff_beginning:len(data)]

        for item in self._find_items([path], _callback, include_toplevel=True,
                                     include_children=False, recurse=False):
            if item:
                yield item

    def test(self, path, exists=False, directory=False, zero_length=False):
        """Test if a path exist, is a directory or has zero length

        :param path: Path to test
        :type path: string
        :param exists: Check if the path exists
        :type exists: boolean
        :param directory: Check if the path is a directory
        :type directory: boolean
        :param zero_length: Check if the path is zero-length
        :type zero_length: boolean
        :returns: a boolean

        .. note:: directory and zero length are AND'd.
        """
        if not isinstance(path, (str, unicode)):
            raise InvalidInputException("Path should be a string")
        if not path:
            raise InvalidInputException("test: no path given")

        def _callback(_path, node):
            if directory and not self._is_dir(node):
                return False
            if zero_length and node.length != 0:
                return False

            return True

        try:
            items = list(self._find_items([path], _callback, include_toplevel=True))
            if len(items) == 0:
                return False
            return all(items)
        except FileNotFoundException as e:
            if exists:
                return False
            else:
                raise e

    def text(self, paths, check_crc=False):
        """ Takes a source file and outputs the file in text format.
        The allowed formats are gzip and bzip2

        :param paths: Paths to display
        :type paths: list of strings
        :param check_crc: Check for checksum errors
        :type check_crc: boolean
        :returns: a generator that yields strings
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("text: no path given")

        def _callback(path, node):
            if self._is_dir(node):
                raise DirectoryException("text: `%s': Is a directory" % path)

            text = ''
            for load in self._read_file(path, node, False, check_crc):
                text += load

            extension = os.path.splitext(path)[1]
            if extension == '.gz':
                return zlib.decompress(text, 16 + zlib.MAX_WBITS)
            elif extension == '.bz2':
                return bz2.decompress(text)
            else:
                return text

        for item in self._find_items(paths, _callback, include_toplevel=True,
                                     include_children=False, recurse=False):
            if item:
                yield item

    def mkdir(self, paths, create_parent=False, mode=0o755):
        """ Create a directoryCount

        :param paths: Paths to create
        :type paths: list of strings
        :param create_parent: Also create the parent directories
        :type create_parent: boolean
        :param mode: Mode the directory should be created with
        :type mode: int
        :returns: a generator that yields dictionaries
        """
        if not isinstance(paths, list):
            raise InvalidInputException("Paths should be a list")
        if not paths:
            raise InvalidInputException("mkdirs: no path given")

        for path in paths:
            if not path.startswith("/"):
                path = self._join_user_path(path)

            fileinfo = self._get_file_info(path)
            if not fileinfo:
                try:
                    request = client_proto.MkdirsRequestProto()
                    request.src = path
                    request.masked.perm = mode
                    request.createParent = create_parent
                    service = self._get_service()
                    response = service.mkdirs(request)
                    yield {"path": path, "result": response.result}
                except RequestError as e:
                    yield {"path": path, "result": False, "error": str(e)}
            else:
                yield {"path": path, "result": False, "error": "mkdir: `%s': File exists" % path}

    def serverdefaults(self, force_reload=False):
        """Get server defaults, caching the results. If there are no results saved, or the force_reload flag is True,
        it will query the HDFS server for its default parameter values. Otherwise, it will simply return the results
        it has already queried.

        Note: This function returns a copy of the results loaded from the server, so you can manipulate or change
        them as you'd like. If for any reason you need to change the results the client saves, you must access
        the property client._server_defaults directly.

        :param force_reload: Should the server defaults be reloaded even if they already exist?
        :type force_reload: bool
        :returns: dictionary with the following keys: blockSize, bytesPerChecksum, writePacketSize, replication, fileBufferSize, encryptDataTransfer, trashInterval, checksumType

        **Example:**

        >>> client.serverdefaults()
        [{'writePacketSize': 65536, 'fileBufferSize': 4096, 'replication': 1, 'bytesPerChecksum': 512, 'trashInterval': 0L, 'blockSize': 134217728L, 'encryptDataTransfer': False, 'checksumType': 2}]

        """

        if not self._server_defaults or force_reload:
            request = client_proto.GetServerDefaultsRequestProto()
            service = self._get_service()
            response = service.getServerDefaults(request).serverDefaults
            self._server_defaults = {
                'blockSize': response.blockSize,
                'bytesPerChecksum': response.bytesPerChecksum,
                'writePacketSize': response.writePacketSize,
                'replication': response.replication,
                'fileBufferSize': response.fileBufferSize,
                'encryptDataTransfer': response.encryptDataTransfer,
                'trashInterval': response.trashInterval,
                'checksumType': response.checksumType
            }

        # return a copy, so if the user changes any values, they won't be saved in the client
        return self._server_defaults.copy()

    def _find_items(self, paths, processor,
                    include_toplevel=False,
                    include_children=False,
                    recurse=False,
                    check_nonexistence=False):
        """ Request file info from the NameNode and call the processor on the node(s) returned

        :param paths:
            A list of paths that need to be processed
        :param processor:
            Method that is called on an node. Method signature should be foo(path, node). For additional
            (static) params, use a lambda.
        :param include_toplevel:
            Boolean to enable the inclusion of the first node found.
            Example: listing a directory should not include the toplevel, but chmod should
            only operate on the path that is input, so it should include the toplevel.
        :param include_children:
            Include children (when the path is a directory) in processing. Recurse will always
            include children.
            Example: listing a directory should include children, but chmod shouldn't.
        :param recurse:
            Recurse into children if they are directories.
        """

        if not paths:
            paths = [posixpath.join("/user", get_current_username())]

        # Expand paths if necessary (/foo/{bar,baz} --> ['/foo/bar', '/foo/baz'])
        paths = glob.expand_paths(paths)

        for path in paths:
            if not path.startswith("/"):
                path = self._join_user_path(path)

            # Normalize path (remove double /, handle '..', remove trailing /, etc)
            path = self._normalize_path(path)

            log.debug("Trying to find path %s" % path)

            if glob.has_magic(path):
                log.debug("Dealing with globs in %s" % path)
                for item in self._glob_find(path, processor, include_toplevel):
                    yield item
            else:
                fileinfo = self._get_file_info(path)
                if not fileinfo and not check_nonexistence:
                    raise FileNotFoundException("`%s': No such file or directory" % path)
                elif not fileinfo and check_nonexistence:
                    yield processor(path, None)
                    continue
                elif fileinfo and check_nonexistence:
                    yield {"path": path, "result": False, "error": "File already exists"}
                    continue

                if (include_toplevel and fileinfo) or not self._is_dir(fileinfo.fs):
                    # Construct the full path before processing
                    full_path = self._get_path_with_node(path, fileinfo.fs)
                    log.debug("Added %s to to result set" % full_path)
                    entry = processor(full_path, fileinfo.fs)
                    yield entry

                if self._is_dir(fileinfo.fs) and (include_children or recurse):
                    for node in self._get_dir_listing(path):
                        full_path = self._get_path_with_node(path, node)
                        entry = processor(full_path, node)
                        yield entry

                        # Recurse into directories
                        if recurse and self._is_dir(node):
                            # Construct the full path before processing
                            full_path = posixpath.join(path, node.path)
                            for item in self._find_items([full_path],
                                                         processor,
                                                         include_toplevel=False,
                                                         include_children=False,
                                                         recurse=recurse):
                                yield item

    @staticmethod
    def _join_user_path(path):
        return posixpath.join("/user", get_current_username(), path)

    @staticmethod
    def _get_path_with_node(path, node):
        if node.path:
            return posixpath.join(path, node.path)
        else:
            return path

    def _read_file(self, path, node, tail_only, check_crc, tail_length=1024):
        length = node.length

        request = client_proto.GetBlockLocationsRequestProto()
        request.src = path
        request.length = length

        if tail_only:  # Only read last part, default is 1KB
            request.offset = max(0, length - tail_length)
        else:
            request.offset = long(0)

        service = self._get_service()
        response = service.getBlockLocations(request)

        if response.locations.fileLength == 0:  # Can't read empty file
            yield ""
        lastblock = response.locations.lastBlock

        if tail_only:
            # we assume that tail_length <= default block size due to check in Client.tail
            if lastblock.b.blockId == response.locations.blocks[0].b.blockId:
                num_blocks_tail = 1  # Tail is on last block
            else:
                num_blocks_tail = 2  # Tail is on two blocks

        failed_nodes = []
        total_bytes_read = 0
        for block in response.locations.blocks:
            length = block.b.numBytes
            pool_id = block.b.poolId
            offset_in_block = 0
            block_token = block.blockToken

            if tail_only:
                if num_blocks_tail == 2 and block.b.blockId != lastblock.b.blockId:
                    offset_in_block = block.b.numBytes - (tail_length - lastblock.b.numBytes)
                elif num_blocks_tail == 1:
                    offset_in_block = max(0, lastblock.b.numBytes - tail_length)

            # Prioritize locations to read from
            locations_queue = Queue.PriorityQueue()  # Primitive queuing based on a node's past failure
            for location in block.locs:
                if location.id.storageID in failed_nodes:
                    locations_queue.put((1, location))  # Priority num, data
                else:
                    locations_queue.put((0, location))

            # Read data
            successful_read = False
            while not locations_queue.empty():
                location = locations_queue.get()[1]
                host = location.id.hostName if self.use_datanode_hostname else location.id.ipAddr
                port = int(location.id.xferPort)
                data_xciever = DataXceiverChannel(host, port)
                if data_xciever.connect():
                    try:
                        for load in data_xciever.readBlock(length, pool_id, block.b.blockId, block.b.generationStamp, offset_in_block, block_token, check_crc):
                            offset_in_block += len(load)
                            total_bytes_read += len(load)
                            successful_read = True
                            yield load
                    except Exception as e:
                        log.getChild('transient').error(e)
                        if not location.id.storageID in failed_nodes:
                            failed_nodes.append(location.id.storageID)
                        successful_read = False
                else:
                    raise ConnectionFailureException(
                        u"Failure to connect to data node at ({}:{})".format(
                            host, port
                        ))
                if successful_read:
                    break
            if successful_read is False:
                raise TransientException("Failure to read block %s" % block.b.blockId)

    def _get_dir_listing(self, path, start_after=''):
        request = client_proto.GetListingRequestProto()
        request.src = path
        request.startAfter = start_after
        request.needLocation = False
        service = self._get_service()
        listing = service.getListing(request)
        if not listing:
            return
        for node in listing.dirList.partialListing:
            start_after = node.path
            yield node
        if listing.dirList.remainingEntries > 0:
            for node in self._get_dir_listing(path, start_after):
                yield node

    def _glob_find(self, path, processor, include_toplevel):
        """Handle globs in paths.
        This is done by listing the directory before a glob and checking which
        node matches the initial glob. If there are more globs in the path,
        we don't add the found children to the result, but traverse into paths
        that did have a match.
        """

        # Split path elements and check where the first occurence of magic is
        path_elements = path.split("/")
        first_magic = None
        for i, element in enumerate(path_elements):
            if glob.has_magic(element):
                first_magic = i
                break

        # Create path that we check first to get a listing we match all children
        # against. If the 2nd path element is a glob, we need to check "/", and
        # we hardcode that, since "/".join(['']) doesn't return "/"
        if first_magic == 1:
            check_path = "/"
        else:
            check_path = "/".join(path_elements[:first_magic])

        # Path that we need to match against
        match_path = "/".join(path_elements[:first_magic + 1])

        # Rest of the unmatched path. In case the rest is only one element long
        # we prepend it with "/", since "/".join(['x']) doesn't return "/x"
        rest_elements = path_elements[first_magic + 1:]
        if len(rest_elements) == 1:
            rest = rest_elements[0]
        else:
            rest = "/".join(rest_elements)
        # Check if the path exists and that it's a directory (which it should..)
        fileinfo = self._get_file_info(check_path)
        if fileinfo and self._is_dir(fileinfo.fs):
            # List all child nodes and match them agains the glob
            for node in self._get_dir_listing(check_path):
                full_path = self._get_path_with_node(check_path, node)
                if fnmatch.fnmatch(full_path, match_path):
                    # If we have a match, but need to go deeper, we recurse
                    if rest and glob.has_magic(rest):
                        traverse_path = "/".join([full_path, rest])
                        for item in self._glob_find(traverse_path, processor, include_toplevel):
                            yield item
                    elif rest:
                        # we have more rest, but it's not magic, which is either a file or a directory
                        final_path = posixpath.join(full_path, rest)
                        fi = self._get_file_info(final_path)
                        if fi and self._is_dir(fi.fs):
                            for n in self._get_dir_listing(final_path):
                                full_child_path = self._get_path_with_node(final_path, n)
                                yield processor(full_child_path, n)
                        elif fi:
                            yield processor(final_path, fi.fs)
                    else:
                        # If the matching node is a directory, we list the directory
                        # This is what the hadoop client does at least.
                        if self._is_dir(node):
                            if include_toplevel:
                                yield processor(full_path, node)
                            fp = self._get_path_with_node(check_path, node)
                            dir_list = self._get_dir_listing(fp)
                            if dir_list:  # It might happen that the directory above has been removed
                                for n in dir_list:
                                    full_child_path = self._get_path_with_node(fp, n)
                                    yield processor(full_child_path, n)
                        else:
                            yield processor(full_path, node)

    def _is_dir(self, entry):
        return self.FILETYPES.get(entry.fileType) == "d"

    def _is_file(self, entry):
        return self.FILETYPES.get(entry.fileType) == "f"

    def _get_file_info(self, path):
        request = client_proto.GetFileInfoRequestProto()
        request.src = path
        service = self._get_service()
        return service.getFileInfo(request)

    @staticmethod
    def _normalize_path(path):
        return posixpath.normpath(re.sub('/+', '/', path))

    def _get_service(self):
        pass
