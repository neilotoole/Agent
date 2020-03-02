/*******************************************************************************
 * Copyright (c) 2018 Edgeworx, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 * Saeid Baghbidi
 * Kilton Hopkins
 *  Ashita Nagar
 *******************************************************************************/
package org.eclipse.iofog.process_manager;

import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Image;
import org.eclipse.iofog.microservice.*;
import org.eclipse.iofog.exception.AgentSystemException;
import org.eclipse.iofog.network.IOFogNetworkInterface;
import org.eclipse.iofog.status_reporter.StatusReporter;
import org.eclipse.iofog.utils.Constants;
import org.eclipse.iofog.utils.logging.LoggingService;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static org.eclipse.iofog.microservice.Microservice.deleteLock;

/**
 * provides methods to manage Docker containers
 *
 * @author saeid
 */
public class ContainerManager {

	private DockerUtil docker;
	private final MicroserviceManager microserviceManager;

	private static final String MODULE_NAME = "Container Manager";

	public ContainerManager() {
		microserviceManager = MicroserviceManager.getInstance();
	}

	/**
	 * pulls {@link Image} from {@link Registry} and creates a new {@link Container}
	 *
	 * @throws Exception exception
	 */
	private CompletableFuture<Void> addContainer(Microservice microservice) throws Exception {
		CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
		LoggingService.logInfo(MODULE_NAME, "Start pull image from registry and creates a new container");
		Optional<Container> containerOptional = docker.getContainer(microservice.getMicroserviceUuid());
		if (!containerOptional.isPresent()) {
			LoggingService.logInfo(MODULE_NAME, "creating \"" + microservice.getImageName() + "\"");
			completableFuture = createContainer(microservice);
		}
		LoggingService.logInfo(MODULE_NAME, "Finished pull image from registry and creates a new container");

		return completableFuture;
	}

	private Registry getRegistry(Microservice microservice) throws AgentSystemException {
		LoggingService.logInfo(MODULE_NAME, "Start get registry");
		Registry registry;
		registry = microserviceManager.getRegistry(microservice.getRegistryId());
		if (registry == null) {
			throw new AgentSystemException(String.format("registry is not valid \"%d\"", microservice.getRegistryId()), null);
		}
		LoggingService.logInfo(MODULE_NAME, "Finished get registry");
		return registry;
	}

	/**
	 * removes an existing {@link Container} and creates a new one
	 *
	 * @param withCleanUp if true then removes old image and volumes
	 * @throws Exception exception
	 */
	private CompletableFuture<Void> updateContainer(Microservice microservice, boolean withCleanUp) throws Exception {
		LoggingService.logInfo(MODULE_NAME, "Start update container");
		microservice.setUpdating(true);
		return removeContainerByMicroserviceUuid(microservice.getMicroserviceUuid(), withCleanUp)
				.thenCombineAsync(createContainer(microservice), (v1, v2) -> null)
				.handleAsync((v, e) -> {
					microservice.setUpdating(false);
					LoggingService.logInfo(MODULE_NAME, "Finished update container");
					return null;
				});
	}

	private CompletableFuture<Void> createContainer(Microservice microservice) throws Exception {
		return createContainer(microservice, true);
	}

	private CompletableFuture<Void> createContainer(Microservice microservice, boolean pullImage) throws Exception {
		CompletableFuture<Void> pullImageAsync = CompletableFuture.completedFuture(null);

		setMicroserviceStatus(microservice.getMicroserviceUuid(), MicroserviceState.PULLING);
		Registry registry = getRegistry(microservice);
		if (!registry.getUrl().equals("from_cache") && pullImage){
			LoggingService.logInfo(MODULE_NAME, "pulling \"" + microservice.getImageName() + "\" from registry");
			pullImageAsync = CompletableFuture
					.supplyAsync(docker.pullImage(microservice.getImageName(), registry))
					.exceptionally(e -> {
						LoggingService.logError(MODULE_NAME, "unable to pull \"" + microservice.getImageName() + "\" from registry. trying local cache",
								new AgentSystemException("unable to pull \"" + microservice.getImageName() + "\" from registry. trying local cache", e));
						return null;
					}).thenApplyAsync(e -> {
						try {
							createContainer(microservice, false).get();
						} catch (Exception ex) {
							throw new CompletionException(ex);
						}
						LoggingService.logInfo(MODULE_NAME, "created \"" + microservice.getImageName() + "\" from local cache");
						return null;
					}).thenApplyAsync((e) -> {
						LoggingService.logInfo(MODULE_NAME, String.format("\"%s\" pulled", microservice.getImageName()));
						return null;
					}).thenApplyAsync((e) -> {
						if (!docker.findLocalImage(microservice.getImageName())) {
							throw new NotFoundException("Image not found in local cache");
						}
						return null;
					});
		}

		String hostName = IOFogNetworkInterface.getCurrentIpAddress();
		return pullImageAsync.thenAcceptAsync((e) -> {
			LoggingService.logInfo(MODULE_NAME, "creating container \"" + microservice.getImageName() + "\"");
			setMicroserviceStatus(microservice.getMicroserviceUuid(), MicroserviceState.STARTING);

			CompletableFuture.supplyAsync(docker.createContainer(microservice, hostName))
				.thenApplyAsync((containerId) -> {
					microservice.setContainerId(containerId);
					try {
						microservice.setContainerIpAddress(docker.getContainerIpAddress(containerId));
					} catch (Exception ex) {
						throw new CompletionException(ex);
					}
					LoggingService.logInfo(MODULE_NAME, "container is created \"" + microservice.getImageName() + "\"");
					microservice.setRebuild(false);
					return CompletableFuture.supplyAsync(startContainer(microservice));
				});
		});
	}

	/**
	 * starts a {@link Container} and sets appropriate status
	 */
	private Supplier<Void> startContainer(Microservice microservice) {
		return () -> {
			LoggingService.logInfo(MODULE_NAME, String.format("trying to start container \"%s\"", microservice.getImageName()));
			try {
				if (!docker.isContainerRunning(microservice.getContainerId())) {
					docker.startContainer(microservice);
				}
				Optional<String> statusOptional = docker.getContainerStatus(microservice.getContainerId());
				String status = statusOptional.orElse("unknown");
				LoggingService.logInfo(MODULE_NAME, String.format("starting %s, status: %s", microservice.getImageName(), status));
				microservice.setContainerIpAddress(docker.getContainerIpAddress(microservice.getContainerId()));
			} catch (Exception ex) {
				LoggingService.logError(MODULE_NAME,
						String.format("Container \"%s\" not found", microservice.getImageName()),
						new AgentSystemException(String.format("Container \"%s\" not found", microservice.getImageName()), ex));
			}
			LoggingService.logInfo(MODULE_NAME, String.format("Finished trying to start container \"%s\"", microservice.getImageName()));

			return null;
		};
	}

	/**
	 * stops a {@link Container}
	 *
	 * @param microserviceUuid id of the {@link Microservice}
	 */
	private CompletableFuture<Void> stopContainer(String microserviceUuid) {
		CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);

		LoggingService.logInfo(MODULE_NAME, "Stop container by microserviceuuid : " + microserviceUuid);
		Optional<Container> containerOptional = docker.getContainer(microserviceUuid);
		if (containerOptional.isPresent()) {
			Container container = containerOptional.get();
			setMicroserviceStatus(microserviceUuid, MicroserviceState.STOPPING);
			LoggingService.logInfo(MODULE_NAME, String.format("Stopping container \"%s\"", container.getId()));
			try {
				completableFuture = CompletableFuture.supplyAsync(docker.stopContainer(container.getId()));
				LoggingService.logInfo(MODULE_NAME, String.format("Container \"%s\" stopped", container.getId()));
			} catch (Exception e) {
				LoggingService.logError(MODULE_NAME, String.format("Error stopping container \"%s\"", container.getId()),
						new AgentSystemException(String.format("Error stopping container \"%s\"", container.getId()), e));
			}
		}
		setMicroserviceStatus(microserviceUuid, MicroserviceState.STOPPED);
		LoggingService.logInfo(MODULE_NAME, "Stopped container by microserviceuuid : " + microserviceUuid);

		return completableFuture;
	}

	/**
	 * removes a {@link Container} by Microservice uuid
	 */
	private CompletableFuture<Void> removeContainerByMicroserviceUuid(String microserviceUuid, boolean withCleanUp) throws AgentSystemException {
		CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);

		LoggingService.logInfo(MODULE_NAME, "Start remove container by microserviceuuid : " + microserviceUuid);
		synchronized (deleteLock) {
			Optional<Container> containerOptional = docker.getContainer(microserviceUuid);
			if (containerOptional.isPresent()) {
				Container container = containerOptional.get();
				setMicroserviceStatus(microserviceUuid, MicroserviceState.DELETING);
				completableFuture = stopContainer(microserviceUuid)
						.thenCombineAsync(removeContainer(container.getId(), container.getImageId(), withCleanUp), (v1, v2) -> null);
			}
		}
		LoggingService.logInfo(MODULE_NAME, "Finished remove container by microserviceuuid : " + microserviceUuid);

		return completableFuture;
	}

	private CompletableFuture<Void> removeContainer(String containerId, String imageId, boolean withCleanUp) {
		LoggingService.logInfo(MODULE_NAME, String.format("removing container \"%s\"", containerId));
		CompletableFuture<Void> completableFuture = CompletableFuture.supplyAsync(docker.removeContainer(containerId, withCleanUp));
		if (withCleanUp) {
			completableFuture.thenApplyAsync(v -> CompletableFuture.supplyAsync(docker.removeImageById(imageId)))
					.thenApplyAsync(u -> {
						LoggingService.logInfo(MODULE_NAME, String.format("Container \"%s\" removed", containerId));
						return null;
					}).exceptionally(ex -> {
						LoggingService.logError(MODULE_NAME, String.format("Image for container \"%s\" cannot be removed", containerId),
								new AgentSystemException(String.format("Image for container \"%s\" cannot be removed", containerId), ex));
						return null;
					});
		}

		return completableFuture;
	}

	/**
	 * executes assigned task
	 *
	 * @param task - tasks to be executed
	 */
	public CompletableFuture<Void> execute(ContainerTask task) throws Exception {
		CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
		LoggingService.logInfo(MODULE_NAME, "Start executes assigned task");
		docker = DockerUtil.getInstance();
		if (task != null) {
			Optional<Microservice> microserviceOptional = microserviceManager.findLatestMicroserviceByUuid(task.getMicroserviceUuid());
			switch (task.getAction()) {
				case ADD:
					if (microserviceOptional.isPresent()) {
						completableFuture = addContainer(microserviceOptional.get());
					}
					break;
				case UPDATE:
					if (microserviceOptional.isPresent()) {
						Microservice microservice = microserviceOptional.get();
						completableFuture = updateContainer(microserviceOptional.get(), microservice.isRebuild() && microservice.getRegistryId() != Constants.CACHE_REGISTRY_ID);
					}
					break;
				case REMOVE:
					completableFuture = removeContainerByMicroserviceUuid(task.getMicroserviceUuid(), false);
					break;
				case REMOVE_WITH_CLEAN_UP:
					completableFuture = removeContainerByMicroserviceUuid(task.getMicroserviceUuid(), true);
					break;
				case STOP:
					stopContainerByMicroserviceUuid(task.getMicroserviceUuid());
					break;
			}
		} else {
			LoggingService.logError(MODULE_NAME, "Container Task cannot be null",
					new AgentSystemException("Container Task container be null"));
		}
		LoggingService.logInfo(MODULE_NAME, "Finished executes assigned task");

		return completableFuture;
	}

	private void stopContainerByMicroserviceUuid(String microserviceUuid) {
		LoggingService.logInfo(MODULE_NAME, String.format("stopping container with microserviceId \"%s\"", microserviceUuid));
		stopContainer(microserviceUuid);
	}

	private void setMicroserviceStatus(String uuid, MicroserviceState state) {
		StatusReporter.setProcessManagerStatus().setMicroservicesState(uuid, state);
	}
}
