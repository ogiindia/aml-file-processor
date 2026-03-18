package com.aml.file.pro.core.efrmsrv.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.aml.file.pro.core.efrmsrv.entity.Alerts;


@Repository
public interface AlertsRepo extends JpaRepository<Alerts, Integer> {

}
