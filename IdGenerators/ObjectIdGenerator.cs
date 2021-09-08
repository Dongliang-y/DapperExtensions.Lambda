/* Copyright 2010-present MongoDB Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using MongoDB.Bson.Serialization;

namespace DapperExtensions.IdGenerators
{
    /// <summary>
    /// Represents an Id generator for ObjectIds.
    /// </summary>
    public class ObjectIdGenerator 
    {
        // private static fields
        private static ObjectIdGenerator __instance = new ObjectIdGenerator();

        // constructors
        /// <summary>
        /// Initializes a new instance of the ObjectIdGenerator class.
        /// </summary>
        public ObjectIdGenerator()
        {
        }

        // public static properties
        /// <summary>
        /// Gets an instance of ObjectIdGenerator.
        /// </summary>
        public static ObjectIdGenerator Instance
        {
            get
            {
                return __instance;
            }
        }

        // public methods
        /// <summary>
        /// Generates an Id for a document.
        /// </summary>
        /// <returns>An Id.</returns>
        public object GenerateId()
        {
            return ObjectId.GenerateNewId();
        }

        /// <summary>
        /// Tests whether an Id is empty.
        /// </summary>
        /// <param name="id">The Id.</param>
        /// <returns>True if the Id is empty.</returns>
        public bool IsEmpty(object id)
        {
            return id == null || (ObjectId)id == ObjectId.Empty;
        }
    }
}