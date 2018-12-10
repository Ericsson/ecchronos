/*
 * Copyright 2018 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.repair;

import java.util.function.Supplier;

/**
 * The type of locking to use for repair jobs.
 */
public enum RepairLockType
{
    DATACENTER(DataCenterRepairResourceFactory::new),
    VNODE(VnodeRepairResourceFactory::new),
    DATACENTER_AND_VNODE(() -> new CombinedRepairResourceFactory(new DataCenterRepairResourceFactory(),
            new VnodeRepairResourceFactory()));

    final Supplier<RepairResourceFactory> myRepairLockingFactoryProvider;

    RepairLockType(Supplier<RepairResourceFactory> repairLockingProvider)
    {
        myRepairLockingFactoryProvider = repairLockingProvider;
    }

    RepairResourceFactory getLockFactory()
    {
        return myRepairLockingFactoryProvider.get();
    }
}
