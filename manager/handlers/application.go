/*
 *     Copyright 2020 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	// nolint
	_ "d7y.io/dragonfly/v2/manager/model"
	"d7y.io/dragonfly/v2/manager/types"
)

// @Summary Create Application
// @Description create by json config
// @Tags Application
// @Accept json
// @Produce json
// @Param Application body types.CreateApplicationRequest true "Application"
// @Success 200 {object} model.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications [post]
func (h *Handlers) CreateApplication(ctx *gin.Context) {
	var json types.CreateApplicationRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	application, err := h.service.CreateApplication(ctx.Request.Context(), json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, application)
}

// @Summary Destroy Application
// @Description Destroy by id
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id} [delete]
func (h *Handlers) DestroyApplication(ctx *gin.Context) {
	var params types.ApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DestroyApplication(ctx.Request.Context(), params.ID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Update Application
// @Description Update by json config
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param Application body types.UpdateApplicationRequest true "Application"
// @Success 200 {object} model.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id} [patch]
func (h *Handlers) UpdateApplication(ctx *gin.Context) {
	var params types.ApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	var json types.UpdateApplicationRequest
	if err := ctx.ShouldBindJSON(&json); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	application, err := h.service.UpdateApplication(ctx.Request.Context(), params.ID, json)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, application)
}

// @Summary Get Application
// @Description Get Application by id
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Success 200 {object} model.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id} [get]
func (h *Handlers) GetApplication(ctx *gin.Context) {
	var params types.ApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	application, err := h.service.GetApplication(ctx.Request.Context(), params.ID)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.JSON(http.StatusOK, application)
}

// @Summary Get Applications
// @Description Get Applications
// @Tags Application
// @Accept json
// @Produce json
// @Param page query int true "current page" default(0)
// @Param per_page query int true "return max item count, default 10, max 50" default(10) minimum(2) maximum(50)
// @Success 200 {object} []model.Application
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications [get]
func (h *Handlers) GetApplications(ctx *gin.Context) {
	var query types.GetApplicationsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	h.setPaginationDefault(&query.Page, &query.PerPage)
	applications, count, err := h.service.GetApplications(ctx.Request.Context(), query)
	if err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	h.setPaginationLinkHeader(ctx, query.Page, query.PerPage, int(count))
	ctx.JSON(http.StatusOK, applications)
}

// @Summary Add Scheduler to Application
// @Description Add Scheduler to Application
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id}/scheduler-clusters/{scheduler_cluster_id} [put]
func (h *Handlers) AddSchedulerClusterToApplication(ctx *gin.Context) {
	var params types.AddSchedulerClusterToApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddSchedulerClusterToApplication(ctx.Request.Context(), params.ID, params.SchedulerClusterID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Delete Scheduler to Application
// @Description Delete Scheduler to Application
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param scheduler_cluster_id path string true "scheduler cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id}/scheduler-clusters/{scheduler_cluster_id} [delete]
func (h *Handlers) DeleteSchedulerClusterToApplication(ctx *gin.Context) {
	var params types.DeleteSchedulerClusterToApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DeleteSchedulerClusterToApplication(ctx.Request.Context(), params.ID, params.SchedulerClusterID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Add SeedPeer to Application
// @Description Add SeedPeer to Application
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param seed_peer_cluster_id path string true "seed peer cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id}/seed-peer-clusters/{seed_peer_cluster_id} [put]
func (h *Handlers) AddSeedPeerClusterToApplication(ctx *gin.Context) {
	var params types.AddSeedPeerClusterToApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.AddSeedPeerClusterToApplication(ctx.Request.Context(), params.ID, params.SeedPeerClusterID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}

// @Summary Delete SeedPeer to Application
// @Description Delete SeedPeer to Application
// @Tags Application
// @Accept json
// @Produce json
// @Param id path string true "id"
// @Param seed_peer_cluster_id path string true "seed peer cluster id"
// @Success 200
// @Failure 400
// @Failure 404
// @Failure 500
// @Router /applications/{id}/seed-peer-clusters/{seed_peer_cluster_id} [delete]
func (h *Handlers) DeleteSeedPeerClusterToApplication(ctx *gin.Context) {
	var params types.DeleteSeedPeerClusterToApplicationParams
	if err := ctx.ShouldBindUri(&params); err != nil {
		ctx.JSON(http.StatusUnprocessableEntity, gin.H{"errors": err.Error()})
		return
	}

	if err := h.service.DeleteSeedPeerClusterToApplication(ctx.Request.Context(), params.ID, params.SeedPeerClusterID); err != nil {
		ctx.Error(err) // nolint: errcheck
		return
	}

	ctx.Status(http.StatusOK)
}
