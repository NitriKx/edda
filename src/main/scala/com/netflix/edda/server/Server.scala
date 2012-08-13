/*
 *
 *  Copyright 2012 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.edda.server;

import javax.servlet.http.HttpServlet

import org.slf4j.{Logger,LoggerFactory}

object Server {
    val logger = LoggerFactory.getLogger(classOf[Server]);
}

class Server extends HttpServlet {

    override
    def init = {
        super.init
    }

    override 
    def destroy = {
        super.destroy
    }
}