#!/bin/bash
modprobe ib_core
modprobe ib_uverbs
modprobe rdma_cm
modprobe rdma_ucm
modprobe mlx5_ib
modprobe mlx5_core
modprobe mlx4_ib
modprobe mlx4_en
modprobe devlink
modprobe mlx_compat
modprobe ib_ipoib
modprobe ib_ucm
modprobe ib_umad
