# -*- coding: utf-8 -*-
# Copyright 2017 New Vector Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ._base import Config


class UserDirectoryConfig(Config):
    """User Directory Configuration
    Configuration for the behaviour of the /user_directory API
    """

    def read_config(self, config):
        self.user_directory_search_all_users = False
        user_directory_config = config.get("user_directory", None)
        if user_directory_config:
            self.user_directory_search_all_users = (
                user_directory_config.get("search_all_users", False)
            )

    def default_config(self, config_dir_path, server_name, **kwargs):
        return """
        # User Directory configuration
        #
        # 'search_all_users' defines whether to search all users visible to your HS
        # when searching the user directory, rather than limiting to users visible
        # in public rooms.  Defaults to false.  If you set it True, you'll have to run
        # UPDATE user_directory_stream_pos SET stream_id = NULL;
        # on your database to tell it to rebuild the user_directory search indexes.
        #
        #user_directory:
        #   search_all_users: false
        """
