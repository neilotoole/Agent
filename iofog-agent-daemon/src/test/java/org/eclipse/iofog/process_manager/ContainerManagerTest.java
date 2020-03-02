/*******************************************************************************
 * Copyright (c) 2019 Edgeworx, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 * Saeid Baghbidi
 * Kilton Hopkins
 * Neha Naithani
 *******************************************************************************/
package org.eclipse.iofog.process_manager;

import com.github.dockerjava.api.exception.ConflictException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.Container;
import org.eclipse.iofog.exception.AgentSystemException;
import org.eclipse.iofog.microservice.Microservice;
import org.eclipse.iofog.microservice.MicroserviceManager;
import org.eclipse.iofog.microservice.MicroserviceState;
import org.eclipse.iofog.microservice.Registry;
import org.eclipse.iofog.network.IOFogNetworkInterface;
import org.eclipse.iofog.status_reporter.StatusReporter;
import org.eclipse.iofog.utils.logging.LoggingService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author nehanaithani
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ContainerManager.class, MicroserviceManager.class, ContainerTask.class, LoggingService.class,
        DockerUtil.class, Microservice.class, Container.class, StatusReporter.class, ProcessManagerStatus.class,
        Registry.class, IOFogNetworkInterface.class})
public class ContainerManagerTest {
    private ContainerManager containerManager;
    private MicroserviceManager microserviceManager;
    private ContainerTask containerTask;
    private ProcessManagerStatus processManagerStatus;
    private DockerUtil dockerUtil;
    private String MODULE_NAME;
    private Microservice microservice;
    private Container container;
    private Registry registry;
    private Optional<Container> optionalContainer;
    private Optional<Microservice> optionalMicroservice;

    @Before
    public void setUp() throws Exception {
        MODULE_NAME = "Container Manager";
        microserviceManager = mock(MicroserviceManager.class);
        containerTask = mock(ContainerTask.class);
        dockerUtil = mock(DockerUtil.class);
        microservice = mock(Microservice.class);
        container = mock(Container.class);
        registry = mock(Registry.class);
        processManagerStatus = mock(ProcessManagerStatus.class);
        optionalContainer = Optional.of(container);
        optionalMicroservice = Optional.of(microservice);
        PowerMockito.mockStatic(MicroserviceManager.class);
        PowerMockito.mockStatic(LoggingService.class);
        PowerMockito.mockStatic(DockerUtil.class);
        PowerMockito.mockStatic(StatusReporter.class);
        PowerMockito.mockStatic(IOFogNetworkInterface.class);
        PowerMockito.when(MicroserviceManager.getInstance()).thenReturn(microserviceManager);
        PowerMockito.when(DockerUtil.getInstance()).thenReturn(dockerUtil);
        PowerMockito.when(StatusReporter.setProcessManagerStatus()).thenReturn(processManagerStatus);
        PowerMockito.when(IOFogNetworkInterface.getCurrentIpAddress()).thenReturn("url");
        PowerMockito.when(processManagerStatus.setMicroservicesState(any(), any())).thenReturn(processManagerStatus);
        containerManager = PowerMockito.spy(new ContainerManager());
    }

    @After
    public void tearDown() throws Exception {
        reset(containerManager, containerTask, dockerUtil, microserviceManager);
        MODULE_NAME = null;
    }

    /**
     * Test execute when containerTask is null
     */
    @Test
    public void testExecuteWhenContainerTaskIsNull() {
        try {
            containerManager.execute(null).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            Mockito.verify(microserviceManager, never()).findLatestMicroserviceByUuid(any());
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is remove And microservice is Empty
     */
    @Test
    public void testExecuteWhenContainerTaskIsNotNullAndMicroserviceIsEmpty() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE);
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq(containerTask.getMicroserviceUuid()));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq(containerTask.getMicroserviceUuid()), eq(false));
            PowerMockito.verifyPrivate(containerManager, Mockito.never()).invoke("removeContainer", anyString(), anyString(), anyBoolean());
            PowerMockito.verifyPrivate(containerManager, Mockito.never()).invoke("setMicroserviceStatus", anyString(), any(MicroserviceState.class));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is remove And microservice is not empty
     * Task contains microserviceId which is not valid or already removed
     */
    @Test
    public void testExecuteWhenContainerTaskRemoveMicroserviceIdNotValidAndMicroserviceIsNotEmpty() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            verify(dockerUtil).getContainer(any());
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(false));
            PowerMockito.verifyPrivate(containerManager, Mockito.never()).invoke("removeContainer", anyString(), anyString(), anyBoolean());
            PowerMockito.verifyPrivate(containerManager, Mockito.never()).invoke("setMicroserviceStatus", anyString(), any(MicroserviceState.class));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is remove And microservice is not empty
     * Task contains microserviceId which is valid and not already removed
     */
    @Test
    public void testExecuteWhenContainerTaskRemoveMicroserviceIdIsValid() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(container.getId()).thenReturn("containerId");
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            verify(dockerUtil).stopContainer(any());
            verify(dockerUtil, times(2)).getContainer(any());
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("stopContainer",  eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq("containerId"), eq(null), eq(false));
            PowerMockito.verifyPrivate(containerManager, Mockito.times(3)).invoke("setMicroserviceStatus", any(), any(MicroserviceState.class));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is remove And microservice is not empty
     * Task contains microserviceId which is valid and not already removed
     * docker.stopContainer throws Exception
     */
    @Test
    public void throwsExceptionWhenDockerStopContainerIsCalledInExecuteWhenContainerTaskRemoveMicroserviceIdISIsValid() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            PowerMockito.when(container.getId()).thenReturn("containerID");
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            verify(dockerUtil).stopContainer(any());
            verify(dockerUtil, times(2)).getContainer(any());
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("stopContainer",  eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq("containerID"), eq(null), eq(false));
            PowerMockito.verifyPrivate(containerManager, Mockito.times(3))
                    .invoke("setMicroserviceStatus", any(), any(MicroserviceState.class));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is remove And microservice is not empty
     * Task contains microserviceId which is valid and not already removed
     * docker.removeContainer throws Exception
     */
    @Test (expected = NotModifiedException.class)
    public void throwsExceptionWhenDockerRemoveContainerIsCalledInExecuteWhenContainerTaskRemove() throws Exception{
        PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE);
        PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
        PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
        PowerMockito.when(container.getId()).thenReturn("containerId");
        PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
        PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> null);
        PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenThrow(new NotModifiedException("error"));
        containerManager.execute(containerTask).get();
    }

    /**
     * Test execute when containerTask is not null
     * TasK is REMOVE_WITH_CLEAN_UP
     */
    @Test
    public void testExecuteWhenContainerTaskRemoveWithCleanup() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE_WITH_CLEAN_UP);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            PowerMockito.when(container.getId()).thenReturn("containerID");
            PowerMockito.when(container.getImageId()).thenReturn("imageID");
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            verify(dockerUtil).stopContainer(any());
            verify(dockerUtil, times(2)).getContainer(any());
            verify(dockerUtil).removeImageById(any());
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(true));
            PowerMockito.verifyPrivate(containerManager).invoke("stopContainer",  eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq("containerID"), eq("imageID"), eq(true));
            PowerMockito.verifyPrivate(containerManager, Mockito.times(3))
                    .invoke("setMicroserviceStatus", any(), any(MicroserviceState.class));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is REMOVE_WITH_CLEAN_UP
     * docker.removeImageById throws NotFoundException
     */
    @Test
    public void throwsExceptionWhenDockerRemoveImageByIdWhenContainerTaskRemoveWithCleanup() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE_WITH_CLEAN_UP);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(container.getId()).thenReturn("containerID");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> { throw new NotFoundException("error"); });
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            verify(dockerUtil).stopContainer(any());
            verify(dockerUtil, times(2)).getContainer(any());
            verify(dockerUtil).removeImageById(any());
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(true));
            PowerMockito.verifyPrivate(containerManager).invoke("stopContainer",  eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq("containerID"), eq(null), eq(true));
            PowerMockito.verifyPrivate(containerManager, Mockito.times(3))
                    .invoke("setMicroserviceStatus", any(), any(MicroserviceState.class));
            PowerMockito.verifyStatic(LoggingService.class);
            LoggingService.logError(eq(MODULE_NAME), eq("Image for container \"containerID\" cannot be removed"), any());
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is REMOVE_WITH_CLEAN_UP
     * docker.removeImageById throws ConflictException
     */
    @Test
    public void throwsConflictExceptionWhenDockerRemoveImageByIdWhenContainerTaskRemoveWithCleanup() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.REMOVE_WITH_CLEAN_UP);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(container.getId()).thenReturn("containerID");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(dockerUtil.removeImageById(any())).thenThrow(new ConflictException("error"));
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            verify(dockerUtil).stopContainer(any());
            verify(dockerUtil, times(2)).getContainer(any());
            verify(dockerUtil).removeImageById(any());
            Mockito.verify(microserviceManager).findLatestMicroserviceByUuid(eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(true));
            PowerMockito.verifyPrivate(containerManager).invoke("stopContainer",  eq("uuid"));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq("containerID"), eq(null), eq(true));
            PowerMockito.verifyPrivate(containerManager, Mockito.times(3))
                    .invoke("setMicroserviceStatus", any(), any(MicroserviceState.class));
            PowerMockito.verifyStatic(LoggingService.class);
            LoggingService.logError(eq(MODULE_NAME), eq("Image for container \"containerID\" cannot be removed"), any());
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is ADD
     * Microservice is Empty
     */
    @Test
    public void testExecuteWhenContainerTaskAddAndMicroserviceIsEmpty() {
        try {
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.ADD);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            containerManager.execute(containerTask).get();
            PowerMockito.verifyStatic(DockerUtil.class);
            DockerUtil.getInstance();
            PowerMockito.verifyPrivate(containerManager, never()).invoke("addContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager, never()).invoke("createContainer", any());
            PowerMockito.verifyPrivate(containerManager, never()).invoke("getRegistry", any());
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is ADD
     * Microservice is not Empty
     * getRegistries throws AgentSystemException
     * registries from microserviceManager is null
     */
    @Test (expected = AgentSystemException.class)
    public void throwsAgentSystemExceptionWhenRegistriesIsNullExecuteWhenContainerTaskAdd() throws Exception{
        PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                .thenReturn(optionalMicroservice);
        PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.ADD);
        PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
        PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
        containerManager.execute(containerTask).get();
    }

    /**
     * Test execute when containerTask is not null
     * TasK is ADD
     * Microservice is not Empty
     * getRegistries returns registry with url from_cache
     */
    @Test
    public void testExecuteWhenContainerTaskAddAndRegistriesArePresentWithURLFromCache() {
        try {
            PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                    .thenReturn(optionalMicroservice);
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.ADD);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(dockerUtil.createContainer(any(), anyString())).thenReturn(() -> null);
            PowerMockito.when(microserviceManager.getRegistry(anyInt())).thenReturn(registry);
            PowerMockito.when(registry.getUrl()).thenReturn("from_cache");
            containerManager.execute(containerTask).get();
            verify(dockerUtil, never()).pullImage(any(), any());
            verify(dockerUtil).createContainer(any(), any());
            verify(microservice).setRebuild(anyBoolean());
            Mockito.verify(dockerUtil).getContainer(eq(microservice.getMicroserviceUuid()));
            PowerMockito.verifyPrivate(containerManager).invoke("addContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager).invoke("getRegistry", eq(microservice));
            PowerMockito.verifyPrivate(containerManager).invoke("startContainer", eq(microservice));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is ADD
     * Microservice is not Empty
     * getRegistries returns registry with url url
     */
    @Test
    public void testExecuteWhenContainerTaskAddAndRegistriesArePresentWithURL() {
        try {
            PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                    .thenReturn(optionalMicroservice);
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.ADD);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(dockerUtil.createContainer(any(), anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.pullImage(anyString(), any())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.getContainerStatus(anyString())).thenReturn(Optional.of("running"));
            PowerMockito.when(dockerUtil.findLocalImage(anyString())).thenReturn(true);
            PowerMockito.when(microserviceManager.getRegistry(anyInt())).thenReturn(registry);
            PowerMockito.when(registry.getUrl()).thenReturn("url");
            PowerMockito.when(microservice.getImageName()).thenReturn("imageName");
            containerManager.execute(containerTask).get();
            verify(dockerUtil).pullImage(any(), any());
            verify(dockerUtil, atMost(2)).createContainer(any(), any());
            verify(microservice, atMost(2)).setRebuild(anyBoolean());
            Mockito.verify(dockerUtil).getContainer(eq(microservice.getMicroserviceUuid()));
            PowerMockito.verifyPrivate(containerManager).invoke("addContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager, atMost(2)).invoke("getRegistry", eq(microservice));
            PowerMockito.verifyPrivate(containerManager, atMost(2)).invoke("startContainer", eq(microservice));
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is ADD
     * Microservice is not Empty
     * getRegistries returns registry with url
     * Docker.pullImage throws Exception
     * docker.findLocalImage returns false
     */
    @Test (expected = ExecutionException.class)
    public void throwsExceptionWhenDockerImagePullIsCalledExecuteWhenContainerTaskAdd() throws Exception {
        PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                .thenReturn(optionalMicroservice);
        PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.ADD);
        PowerMockito.when(microservice.getImageName()).thenReturn("microserviceName");
        PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
        PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
        PowerMockito.when(dockerUtil.findLocalImage(anyString())).thenReturn(false);
        PowerMockito.when(dockerUtil.pullImage(any(), any())).thenReturn(() -> { throw new CompletionException(new AgentSystemException("error")); });
        PowerMockito.when(microserviceManager.getRegistry(anyInt())).thenReturn(registry);
        PowerMockito.when(registry.getUrl()).thenReturn("url");
        containerManager.execute(containerTask).get();
    }

    /**
     * Test execute when containerTask is not null
     * TasK is ADD
     * Microservice is not Empty
     * getRegistries returns registry with url
     * Docker.pullImage throws Exception
     * docker.findLocalImage returns true
     */
    @Test
    public void testWhenDockerImagePullIsCalledExecuteWhenContainerTaskAdd() {
        try {
            PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                    .thenReturn(optionalMicroservice);
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.ADD);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(microservice.getImageName()).thenReturn("microserviceName");
            PowerMockito.when(dockerUtil.findLocalImage(anyString())).thenReturn(true);
            PowerMockito.when(dockerUtil.pullImage(any(), any())).thenReturn(() -> { throw new CompletionException(new AgentSystemException("error")); });
            PowerMockito.when(dockerUtil.createContainer(any(), any())).thenReturn(() -> null);
            PowerMockito.when(microserviceManager.getRegistry(anyInt())).thenReturn(registry);
            PowerMockito.when(registry.getUrl()).thenReturn("url");
            containerManager.execute(containerTask).get();
            verify(dockerUtil).pullImage(any(), any());
            verify(dockerUtil, atMost(2)).createContainer(any(), any());
            verify(microservice, atMost(2)).setRebuild(anyBoolean());
            PowerMockito.verifyPrivate(containerManager).invoke("addContainer",  eq(microservice));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer",  eq(microservice));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer", eq(microservice), eq(true));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer", eq(microservice), eq(false));
            PowerMockito.verifyPrivate(containerManager, times(2)).invoke("getRegistry",  eq(microservice));
            PowerMockito.verifyPrivate(containerManager, atMost(2)).invoke("startContainer", any());
            PowerMockito.verifyStatic(LoggingService.class);
            LoggingService.logError(eq(MODULE_NAME),
                    eq("unable to pull \"microserviceName\" from registry. trying local cache"),
                    any());
        } catch (Exception e) {
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is UPDATE
     * Microservice is not Empty
     * getRegistries returns registry with url
     * Docker.pullImage throws Exception
     * docker.findLocalImage returns true
     * Microservice isRebuild is false
     * withCleanUp is false
     */
    @Test
    public void testExecuteWhenContainerTaskUpdate() {
        try {
            PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                    .thenReturn(optionalMicroservice);
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.UPDATE);
            PowerMockito.when(microservice.getImageName()).thenReturn("microserviceName");
            PowerMockito.when(microservice.isRebuild()).thenReturn(false);
            PowerMockito.when(microservice.getRegistryId()).thenReturn(2);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(microservice.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.when(microserviceManager.getRegistry(anyInt())).thenReturn(registry);
            PowerMockito.when(registry.getUrl()).thenReturn("url");
            PowerMockito.when(container.getId()).thenReturn("containerId");
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.findLocalImage(anyString())).thenReturn(true);
            PowerMockito.when(dockerUtil.pullImage(any(), any())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.createContainer(any(), any())).thenReturn(() -> null);
            containerManager.execute(containerTask).get();
            verify(dockerUtil).pullImage(any(), any());
            verify(dockerUtil, atMost(2)).createContainer(any(), any());
            verify(microservice, atMost(2)).setRebuild(anyBoolean());
            PowerMockito.verifyPrivate(containerManager).invoke("updateContainer", eq(microservice), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq("containerId"), eq(null), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager, atMost(3)).invoke("startContainer", eq(microservice));
        } catch (Exception e) {
            System.out.println(e);
            fail("This should not happen");
        }
    }

    /**
     * Test execute when containerTask is not null
     * TasK is UPDATE
     * Microservice is not Empty
     * getRegistries returns registry with url
     * Docker.pullImage throws Exception
     * docker.findLocalImage returns true
     * Microservice isRebuild is false
     * withCleanUp is false
     * docker.startContainer throws Exception
     */
    @Test
    public void throwsNotFoundExceptionWhenStartContainerIsCalledInExecuteWhenContainerTaskUpdate() {
        try {
            PowerMockito.when(microserviceManager.findLatestMicroserviceByUuid(anyString()))
                    .thenReturn(optionalMicroservice);
            PowerMockito.when(containerTask.getAction()).thenReturn(ContainerTask.Tasks.UPDATE);
            PowerMockito.when(microservice.getImageName()).thenReturn("microserviceName");
            PowerMockito.when(microservice.isRebuild()).thenReturn(false);
            PowerMockito.when(microservice.getRegistryId()).thenReturn(2);
            PowerMockito.when(containerTask.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(microservice.getMicroserviceUuid()).thenReturn("uuid");
            PowerMockito.when(dockerUtil.getContainer(anyString())).thenReturn(optionalContainer);
            PowerMockito.doThrow(mock(NotFoundException.class)).when(dockerUtil).startContainer(any());
            PowerMockito.when(microserviceManager.getRegistry(anyInt())).thenReturn(registry);
            PowerMockito.when(registry.getUrl()).thenReturn("url");
            PowerMockito.when(container.getId()).thenReturn("containerId");
            PowerMockito.when(dockerUtil.stopContainer(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeImageById(anyString())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.removeContainer(anyString(), anyBoolean())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.findLocalImage(anyString())).thenReturn(true);
            PowerMockito.when(dockerUtil.pullImage(any(), any())).thenReturn(() -> null);
            PowerMockito.when(dockerUtil.createContainer(any(), any())).thenReturn(() -> null);
            containerManager.execute(containerTask).get();
            verify(dockerUtil).pullImage(any(), any());
            verify(dockerUtil, atMost(2)).createContainer(any(), any());
            verify(microservice, atMost(2)).setRebuild(anyBoolean());
            PowerMockito.verifyPrivate(containerManager).invoke("updateContainer", eq(microservice), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainerByMicroserviceUuid", eq("uuid"), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("removeContainer", eq(container.getId()), eq(null), eq(false));
            PowerMockito.verifyPrivate(containerManager).invoke("createContainer", eq(microservice));
            PowerMockito.verifyPrivate(containerManager, atMost(2)).invoke("startContainer", eq(microservice));
            PowerMockito.verifyStatic(LoggingService.class, atMost(2));
            LoggingService.logError(eq(MODULE_NAME),
                    eq("Container \"microserviceName\" not found"),
                    any());
        } catch (Exception e) {
            System.out.println(e);
            fail("This should not happen");
        }
    }
}