package com.aml.file.pro.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.aml.file.pro.core.efrmsrv.entity.MapperEntity;


@Repository
public interface MapperRepository<T> extends JpaRepository<MapperEntity, Integer>  {

}