package com.aml.file.pro.core.efrmsrv.repo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.aml.file.pro.core.efrmsrv.entity.AccountDetailsEntity;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaQuery;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;

@Repository
public class AccountDetailsRepoImpl {

	private Logger LOGGER = LoggerFactory.getLogger(AccountDetailsRepoImpl.class);

	@Autowired
	EntityManager em;

	public List<AccountDetailsEntity> getAccountDetailsbyCriteria(String custId) {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<AccountDetailsEntity> cq = cb.createQuery(AccountDetailsEntity.class);

		Root<AccountDetailsEntity> book = cq.from(AccountDetailsEntity.class);
		List<Predicate> predicates = new ArrayList<Predicate>();
		predicates.add(cb.equal(book.get("customerId"), custId));
		// Predicate authorNamePredicate = cb.equal(book.get("acknowledgementNo"),
		// ackNo);
		// Predicate titlePredicate = cb.like(book.get("title"), "%" + title + "%");
		cq.where(predicates.toArray(new Predicate[] {}));

		TypedQuery<AccountDetailsEntity> query = em.createQuery(cq);
		return query.getResultList();

	}

	public String getAccountOpeningAndClosingDateByritiria(String reqId, String accNo, String custId) {
		CriteriaBuilder cb = null;
		CriteriaQuery<AccountDetailsEntity> cq = null;
		Root<AccountDetailsEntity> book = null;
		List<Predicate> predicates = null;
		String openingDate=null;
		String closingDate=null;
		String combinedStr=null;
		TypedQuery<AccountDetailsEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(AccountDetailsEntity.class);
			book = cq.from(AccountDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			
			
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(book.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(book.get("customerId"), custId));
			}
			
			cq.where(predicates.toArray(new Predicate[] {}));

			query = em.createQuery(cq);
			AccountDetailsEntity customerEnityObj = query.getSingleResult();
			if (customerEnityObj != null && customerEnityObj.getAccountOpenedDate() != null) {
				openingDate = customerEnityObj.getAccountOpenedDate();
				if (customerEnityObj != null && customerEnityObj.getAccountClosedDate() != null) {
					
					combinedStr=openingDate+"@"+customerEnityObj.getAccountClosedDate();
				}
				else
				{
					combinedStr=openingDate;
				}
				
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, combinedStr);
			} else {
				combinedStr = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, combinedStr);
			}
		} catch (Exception e) {
			return null;
		} finally {

		}
		return combinedStr;
	}
	
	
	
	public String getAccountOpeningDateByritiria(String reqId, String accNo, String custId) {
		CriteriaBuilder cb = null;
		CriteriaQuery<AccountDetailsEntity> cq = null;
		Root<AccountDetailsEntity> book = null;
		List<Predicate> predicates = null;
		String openingDate=null;
		String closingDate=null;
		String combinedStr=null;
		TypedQuery<AccountDetailsEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(AccountDetailsEntity.class);
			book = cq.from(AccountDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			
			
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(book.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(book.get("customerId"), custId));
			}
			
			cq.where(predicates.toArray(new Predicate[] {}));

			query = em.createQuery(cq);
			AccountDetailsEntity customerEnityObj = query.getSingleResult();
			if (customerEnityObj != null && customerEnityObj.getAccountOpenedDate() != null) {
				combinedStr = customerEnityObj.getAccountOpenedDate();
				
				
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, combinedStr);
			} else {
				combinedStr = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, combinedStr);
			}
		} catch (Exception e) {
			return null;
		} finally {

		}
		return combinedStr;
	}
	
	public AccountDetailsEntity getAccountDetailsByritiria(String reqId, String accNo, String custId) {
		CriteriaBuilder cb = null;
		CriteriaQuery<AccountDetailsEntity> cq = null;
		Root<AccountDetailsEntity> book = null;
		List<Predicate> predicates = null;
		String openingDate=null;
		String closingDate=null;
		AccountDetailsEntity acctDetails=null;
		TypedQuery<AccountDetailsEntity> query = null;
		try {
			cb = em.getCriteriaBuilder();
			cq = cb.createQuery(AccountDetailsEntity.class);
			book = cq.from(AccountDetailsEntity.class);
			predicates = new ArrayList<Predicate>();
			
			
			if (StringUtils.isNotBlank(accNo)) {
				predicates.add(cb.equal(book.get("accountNo"), accNo));
			}
			if (StringUtils.isNotBlank(custId)) {
				predicates.add(cb.equal(book.get("customerId"), custId));
			}
			
			cq.where(predicates.toArray(new Predicate[] {}));

			query = em.createQuery(cq);
			AccountDetailsEntity customerEnityObj = query.getSingleResult();
			if (customerEnityObj != null)
			{
				acctDetails=customerEnityObj;
				
				
				LOGGER.info("REQID : [{}] - retnVal : [{}]", reqId, acctDetails);
			} else {
				acctDetails = null;
				LOGGER.info("REQID : [{}] - result object is NUll, so retnVal : [{}]", reqId, acctDetails);
			}
		} catch (Exception e) {
			return null;
		} finally {

		}
		return acctDetails;
	}
	
	
}