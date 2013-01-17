package org.collectionspace.services.listener;

import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.collectionspace.services.client.workflow.WorkflowClient;
import org.collectionspace.services.common.api.Tools;
import org.collectionspace.services.movement.nuxeo.MovementConstants;
import org.collectionspace.services.nuxeo.util.NuxeoUtils;
import org.nuxeo.ecm.core.api.ClientException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.event.DocumentEventTypes;
import org.nuxeo.ecm.core.event.Event;
import org.nuxeo.ecm.core.event.EventContext;
import org.nuxeo.ecm.core.event.EventListener;
import org.nuxeo.ecm.core.event.impl.DocumentEventContext;

public abstract class AbstractUpdateObjectLocationValues implements EventListener {

    // FIXME: We might experiment here with using log4j instead of Apache Commons Logging;
    // am using the latter to follow Ray's pattern for now
    private final static Log logger = LogFactory.getLog(AbstractUpdateObjectLocationValues.class);
    // FIXME: Make the following message, or its equivalent, a constant usable by all event listeners
    private final static String NO_FURTHER_PROCESSING_MESSAGE =
            "This event listener will not continue processing this event ...";
    private final static GregorianCalendar EARLIEST_COMPARISON_DATE = new GregorianCalendar(1600, 1, 1);
    private final static String RELATIONS_COMMON_SCHEMA = "relations_common"; // FIXME: Get from external constant
    private final static String RELATION_DOCTYPE = "Relation"; // FIXME: Get from external constant
    private final static String SUBJECT_CSID_PROPERTY = "subjectCsid"; // FIXME: Get from external constant
    private final static String OBJECT_CSID_PROPERTY = "objectCsid"; // FIXME: Get from external constant
    private final static String SUBJECT_DOCTYPE_PROPERTY = "subjectDocumentType"; // FIXME: Get from external constant
    private final static String OBJECT_DOCTYPE_PROPERTY = "objectDocumentType"; // FIXME: Get from external constant
    protected final static String COLLECTIONOBJECTS_COMMON_SCHEMA = "collectionobjects_common"; // FIXME: Get from external constant
    private final static String COLLECTIONOBJECT_DOCTYPE = "CollectionObject"; // FIXME: Get from external constant
    protected final static String COMPUTED_CURRENT_LOCATION_PROPERTY = "computedCurrentLocation"; // FIXME: Create and then get from external constant
    protected final static String MOVEMENTS_COMMON_SCHEMA = "movements_common"; // FIXME: Get from external constant
    private final static String MOVEMENT_DOCTYPE = MovementConstants.NUXEO_DOCTYPE;
    private final static String LOCATION_DATE_PROPERTY = "locationDate"; // FIXME: Get from external constant
    protected final static String CURRENT_LOCATION_PROPERTY = "currentLocation"; // FIXME: Get from external constant
    private final static String ACTIVE_DOCUMENT_WHERE_CLAUSE_FRAGMENT =
            "AND (ecm:currentLifeCycleState <> 'deleted') "
            + "AND ecm:isProxy = 0 "
            + "AND ecm:isCheckedInVersion = 0";
    private boolean isAboutToBeRemovedEvent = false;
    private String movementCsidToFilter;
    private String eventType;

    @Override
    public void handleEvent(Event event) throws ClientException {

        if (logger.isTraceEnabled()) {
            logger.trace("\n-----------------------------------------------------\n");
            logger.trace("In handleEvent in UpdateObjectLocationOnMove ...");
            logger.trace("\n-----------------------------------------------------\n");
        }

        EventContext eventContext = event.getContext();
        if (eventContext == null) {
            return;
        }

        if (!(eventContext instanceof DocumentEventContext)) {
            return;
        }
        DocumentEventContext docEventContext = (DocumentEventContext) eventContext;
        DocumentModel docModel = docEventContext.getSourceDocument();

        eventType = event.getName();
        if (logger.isTraceEnabled()) {
            logger.trace("A(n) " + eventType + " event was received by UpdateObjectLocationOnMove ...");
        }

        if (eventType.equals(DocumentEventTypes.ABOUT_TO_REMOVE)) {
            isAboutToBeRemovedEvent = true;
        }

        // If this document event involves a Relation record, does this pertain to
        // a relationship between a Movement record and a CollectionObject record?
        //
        // If not, we're not interested in processing this document event
        // in this event handler, as it will have no bearing on updating a
        // computed current location for a CollectionObject.

        //
        // (The rest of the code flow below is then identical to that which
        // is followed when this document event involves a Movement record.)
        String movementCsid = "";
        if (documentMatchesType(docModel, RELATION_DOCTYPE)) {
            if (logger.isTraceEnabled()) {
                logger.trace("An event involving a Relation document was received by UpdateObjectLocationOnMove ...");
            }
            // Get a Movement CSID from the Relation record.
            //
            // If we can't get it - if this Relation doesn't involve a
            // Movement - then we don't have a pertinent relation record
            // that can be processed by this event listener / handler.)
            movementCsid = getCsidForDesiredDocType(docModel, MOVEMENT_DOCTYPE, COLLECTIONOBJECT_DOCTYPE);
            if (Tools.isBlank(movementCsid)) {
                logger.warn("Could not obtain CSID for Movement record from document event.");
                logger.warn(NO_FURTHER_PROCESSING_MESSAGE);
                return;
            }
            // If this Relation record is about to be (hard) deleted, set aside
            // its CSID so it can filtered out in all subsequent processing.
            if (isAboutToBeRemovedEvent) {
                movementCsidToFilter = movementCsid;
            }
        } else if (documentMatchesType(docModel, MOVEMENT_DOCTYPE)) {
            if (logger.isTraceEnabled()) {
                logger.trace("An event involving a Movement document was received by UpdateObjectLocationOnMove ...");
            }
            // Otherwise, get a Movement CSID directly from the Movement record.
            movementCsid = NuxeoUtils.getCsid(docModel);
            if (Tools.isBlank(movementCsid)) {
                logger.warn("Could not obtain CSID for Movement record from document event.");
                logger.warn(NO_FURTHER_PROCESSING_MESSAGE);
                return;
            }
            // If this Movement record is about to be (hard) deleted, set aside
            // its CSID so it can filtered out in all subsequent processing.
            if (isAboutToBeRemovedEvent) {
                movementCsidToFilter = movementCsid;
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("This event did not involve a document relevant to UpdateObjectLocationOnMove ...");
            }
            return;
        }

        // Note: currently, all Document lifecycle transitions on
        // the relevant doctype(s) are handled by this event handler,
        // not just transitions between 'soft deleted' and active states.
        //
        // We are assuming that we'll want to re-compute current locations
        // for related CollectionObjects on all such transitions, as the
        // semantics of such transitions are opaque to this event handler,
        // because arbitrary workflows can be bound to those doctype(s).
        //
        // If we need to filter out some of those lifecycle transitions,
        // such as excluding transitions to the 'locked' workflow state; or,
        // alternately, if we want to restrict this event handler's
        // scope to handle only transitions into the 'soft deleted' state,
        // we can add additional checks for doing so at this point in the code.

        if (logger.isTraceEnabled()) {
            logger.trace("Movement CSID=" + movementCsid);
        }

        // Find CollectionObject records that are related to this Movement record:
        //
        // Via an NXQL query, get a list of active relation records where:
        // * This movement record's CSID is the subject CSID of the relation,
        //   and its object document type is a CollectionObject doctype;
        // or
        // * This movement record's CSID is the object CSID of the relation,
        //   and its subject document type is a CollectionObject doctype.
        CoreSession coreSession = docEventContext.getCoreSession();
        // Some values below are hard-coded for readability, rather than
        // being obtained from constants.
        String query = String.format(
                "SELECT * FROM %1$s WHERE " // collectionspace_core:tenantId = 1 "
                + "("
                + "  (%2$s:subjectCsid = '%3$s' "
                + "  AND %2$s:objectDocumentType = '%4$s') "
                + " OR "
                + "  (%2$s:objectCsid = '%3$s' "
                + "  AND %2$s:subjectDocumentType = '%4$s') "
                + ")"
                + ACTIVE_DOCUMENT_WHERE_CLAUSE_FRAGMENT,
                RELATION_DOCTYPE, RELATIONS_COMMON_SCHEMA, movementCsid, COLLECTIONOBJECT_DOCTYPE);
        DocumentModelList relationDocModels = coreSession.query(query);
        if (relationDocModels == null || relationDocModels.isEmpty()) {
            // Encountering a Movement record that is not related to any
            // CollectionObject is potentially a normal occurrence, so no
            // error messages are logged here when we stop handling this event.
            if (logger.isTraceEnabled()) {
                logger.trace("This event did not involve a document relevant to UpdateObjectLocationOnMove ...");
                logger.trace("This Movement record does not have any Relations ...");
            }
            return;
        }

        // Iterate through the list of Relation records found and build
        // a list of CollectionObject CSIDs, by extracting the relevant CSIDs
        // from those Relation records.
        String csid;
        Set<String> collectionObjectCsids = new HashSet<String>();
        for (DocumentModel relationDocModel : relationDocModels) {
            csid = getCsidForDesiredDocType(relationDocModel, COLLECTIONOBJECT_DOCTYPE, MOVEMENT_DOCTYPE);
            if (Tools.notBlank(csid)) {
                collectionObjectCsids.add(csid);
            }
        }
        if (collectionObjectCsids == null || collectionObjectCsids.isEmpty()) {
            logger.warn("Could not obtain any CSIDs of related CollectionObject records.");
            logger.warn(NO_FURTHER_PROCESSING_MESSAGE);
            return;
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Found " + collectionObjectCsids.size() + " CSID(s) of related CollectionObject records.");
            }
        }

        // Iterate through the list of CollectionObject CSIDs found.
        // For each CollectionObject, obtain its most recent, related Movement,
        // and update relevant field(s) with values from that Movement record.
        DocumentModel collectionObjectDocModel;
        DocumentModel mostRecentMovementDocModel;
        for (String collectionObjectCsid : collectionObjectCsids) {
            if (logger.isTraceEnabled()) {
                logger.trace("CollectionObject CSID=" + collectionObjectCsid);
            }
            // Verify that the CollectionObject is retrievable.
            collectionObjectDocModel = getDocModelFromCsid(coreSession, collectionObjectCsid);
            if (collectionObjectDocModel == null) {
                continue;
            }
            // Verify that the CollectionObject record is active.
            if (!isActiveDocument(collectionObjectDocModel)) {
                continue;
            }
            // Get the CollectionObject's most recent, related Movement.
            mostRecentMovementDocModel = getMostRecentMovement(coreSession, collectionObjectCsid,
                    isAboutToBeRemovedEvent, movementCsidToFilter);
            if (mostRecentMovementDocModel == null) {
                continue;
            }
            // Update the CollectionObject with values from that Movement.
            collectionObjectDocModel =
                    updateCollectionObjectValuesFromMovement(collectionObjectDocModel, mostRecentMovementDocModel);
            if (logger.isTraceEnabled()) {
                String computedCurrentLocationRefName =
                        (String) collectionObjectDocModel.getProperty(COLLECTIONOBJECTS_COMMON_SCHEMA,
                        COMPUTED_CURRENT_LOCATION_PROPERTY);
                logger.trace("computedCurrentLocation refName after value update=" + computedCurrentLocationRefName);
            }
            coreSession.saveDocument(collectionObjectDocModel);
        }

    }

    // FIXME: Generic methods like many of those below might be split off from
    // this specific event listener/handler, into an event handler utilities
    // class, base classes, or otherwise.
    //
    // FIXME: Identify whether the equivalent of the documentMatchesType utility
    // method is already implemented and substitute a call to the latter if so.
    // This may well already exist.
    /**
     * Identifies whether a document matches a supplied document type.
     *
     * @param docModel a document model.
     * @param docType a document type string.
     * @return true if the document matches the supplied document type; false if
     * it does not.
     */
    protected static boolean documentMatchesType(DocumentModel docModel, String docType) {
        if (docModel == null || Tools.isBlank(docType)) {
            return false;
        }
        if (docModel.getType().startsWith(docType)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Identifies whether a document is an active document; that is, if it is
     * not a versioned record; not a proxy (symbolic link to an actual record);
     * and not in the 'deleted' workflow state.
     *
     * (A note relating the latter: Nuxeo appears to send 'documentModified'
     * events even on workflow transitions, such when records are 'soft deleted'
     * by being transitioned to the 'deleted' workflow state.)
     *
     * @param docModel
     * @return true if the document is an active document; false if it is not.
     */
    protected static boolean isActiveDocument(DocumentModel docModel) {
        if (docModel == null) {
            return false;
        }
        boolean isActiveDocument = false;
        try {
            if (!docModel.isVersion()
                    && !docModel.isProxy()
                    && !docModel.getCurrentLifeCycleState().equals(WorkflowClient.WORKFLOWSTATE_DELETED)) {
                isActiveDocument = true;
            }
        } catch (ClientException ce) {
            logger.warn("Error while identifying whether document is an active document: ", ce);
        }
        return isActiveDocument;
    }

    /**
     * Returns a document model for a record identified by a CSID.
     *
     * @param session a repository session.
     * @param collectionObjectCsid a CollectionObject identifier (CSID)
     * @return a document model for the record identified by the supplied CSID.
     */
    protected static DocumentModel getDocModelFromCsid(CoreSession session, String collectionObjectCsid) {
        DocumentModelList collectionObjectDocModels = null;
        try {
            final String query = "SELECT * FROM "
                    + NuxeoUtils.BASE_DOCUMENT_TYPE
                    + " WHERE "
                    + NuxeoUtils.getByNameWhereClause(collectionObjectCsid);
            collectionObjectDocModels = session.query(query);
        } catch (Exception e) {
            logger.warn("Exception in query to get document model for CollectionObject: ", e);
        }
        if (collectionObjectDocModels == null || collectionObjectDocModels.isEmpty()) {
            logger.warn("Could not get document models for CollectionObject(s).");
            return null;
        } else if (collectionObjectDocModels.size() != 1) {
            logger.debug("Found more than 1 document with CSID=" + collectionObjectCsid);
            return null;
        }
        return collectionObjectDocModels.get(0);
    }

    // FIXME: A quick first pass, using an only partly query-based technique for
    // getting the most recent Movement record related to a CollectionObject,
    // augmented by procedural code.
    //
    // Should be replaced by a more performant method, based entirely, or nearly so,
    // on a query.
    //
    // E.g. the following is a sample CMIS query for retrieving Movement records
    // related to a CollectionObject, which might serve as the basis for that query.
    /*
     "SELECT DOC.nuxeo:pathSegment, DOC.dc:title, REL.dc:title,"
     + "REL.relations_common:objectCsid, REL.relations_common:subjectCsid FROM Movement DOC "
     + "JOIN Relation REL ON REL.relations_common:objectCsid = DOC.nuxeo:pathSegment "
     + "WHERE REL.relations_common:subjectCsid = '5b4c617e-53a0-484b-804e' "
     + "AND DOC.nuxeo:isVersion = false "
     + "ORDER BY DOC.collectionspace_core:updatedAt DESC";
     */
    /**
     * Returns the most recent Movement record related to a CollectionObject.
     *
     * This method currently returns the related Movement record with the latest
     * (i.e. most recent in time) Location Date field value.
     *
     * @param session a repository session.
     * @param collectionObjectCsid a CollectionObject identifier (CSID)
     * @param isAboutToBeRemovedEvent whether the current event involves a
     * record that is slated for removal (hard deletion)
     * @param movementCsidToFilter the CSID of a Movement record slated for
     * deletion. This record should be filtered out, prior to returning the most
     * recent Movement record.
     * @throws ClientException
     * @return the most recent Movement record related to a CollectionObject
     */
    protected static DocumentModel getMostRecentMovement(CoreSession session, String collectionObjectCsid,
            boolean isAboutToBeRemovedEvent, String aboutToBeRemovedMovementCsidToFilter)
            throws ClientException {
        DocumentModel mostRecentMovementDocModel = null;
        // Get Relation records for Movements related to this CollectionObject.
        //
        // Some values below are hard-coded for readability, rather than
        // being obtained from constants.
        String query = String.format(
                "SELECT * FROM %1$s WHERE " // collectionspace_core:tenantId = 1 "
                + "("
                + "  (%2$s:subjectCsid = '%3$s' "
                + "  AND %2$s:objectDocumentType = '%4$s') "
                + " OR "
                + "  (%2$s:objectCsid = '%3$s' "
                + "  AND %2$s:subjectDocumentType = '%4$s') "
                + ")"
                + ACTIVE_DOCUMENT_WHERE_CLAUSE_FRAGMENT,
                RELATION_DOCTYPE, RELATIONS_COMMON_SCHEMA, collectionObjectCsid, MOVEMENT_DOCTYPE);
        if (logger.isTraceEnabled()) {
            logger.trace("query=" + query);
        }
        DocumentModelList relationDocModels = session.query(query);
        if (relationDocModels == null || relationDocModels.isEmpty()) {
            logger.warn("Unexpectedly found no relations to Movement records to/from to this CollectionObject record.");
            return mostRecentMovementDocModel;
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Found " + relationDocModels.size() + " relation(s) to Movement records to/from this CollectionObject record.");
            }
        }
        // Iterate through related movement records, to find the related
        // Movement record with the most recent location date.
        GregorianCalendar mostRecentLocationDate = EARLIEST_COMPARISON_DATE;
        DocumentModel movementDocModel = null;
        Set<String> alreadyProcessedMovementCsids = new HashSet<String>();
        String relatedMovementCsid = "";
        for (DocumentModel relationDocModel : relationDocModels) {
            // Due to the 'OR' operator in the query above, related Movement
            // record CSIDs may reside in either the subject or object CSID fields
            // of the relation record. Whichever CSID value doesn't match the
            // CollectionObject's CSID is inferred to be the related Movement record's CSID.
            relatedMovementCsid = (String) relationDocModel.getProperty(RELATIONS_COMMON_SCHEMA, SUBJECT_CSID_PROPERTY);
            if (relatedMovementCsid.equals(collectionObjectCsid)) {
                relatedMovementCsid = (String) relationDocModel.getProperty(RELATIONS_COMMON_SCHEMA, OBJECT_CSID_PROPERTY);
            }
            if (Tools.isBlank(relatedMovementCsid)) {
                continue;
            }
            // Because of the OR clause used in the query above, there may be
            // two or more Relation records returned in the query results that
            // reference the same Movement record, as either the subject
            // or object of a relation to the same CollectionObject record;
            // we need to filter out those duplicates.
            if (alreadyProcessedMovementCsids.contains(relatedMovementCsid)) {
                continue;
            } else {
                alreadyProcessedMovementCsids.add(relatedMovementCsid);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Movement CSID=" + relatedMovementCsid);
            }
            // If our event involves a Movement record that is about to be
            // (hard) deleted, filter out that record.
            if (isAboutToBeRemovedEvent && Tools.notBlank(aboutToBeRemovedMovementCsidToFilter)) {
                if (relatedMovementCsid.equals(aboutToBeRemovedMovementCsidToFilter)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Skipping about-to-be-deleted Movement record or referenced, related Movement record ...");
                    }
                    continue;
                }
            }
            movementDocModel = getDocModelFromCsid(session, relatedMovementCsid);
            if (movementDocModel == null) {
                continue;
            }

            // Verify that the Movement record is active. This will also exclude
            // versioned Movement records from the computation of the current
            // location, for tenants that are versioning such records.
            if (!isActiveDocument(movementDocModel)) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping this inactive Movement record ...");
                }
                continue;
            }
            GregorianCalendar locationDate =
                    (GregorianCalendar) movementDocModel.getProperty(MOVEMENTS_COMMON_SCHEMA, LOCATION_DATE_PROPERTY);
            if (locationDate == null) {
                continue;
            }
            if (logger.isTraceEnabled()) {
                logger.trace("locationDate=" + locationDate);
            }
            if (locationDate.after(mostRecentLocationDate)) {
                mostRecentLocationDate = locationDate;
                mostRecentMovementDocModel = movementDocModel;
            }
        }
        return mostRecentMovementDocModel;
    }

    /**
     * Returns the CSID for a desired document type from a Relation record,
     * where the relationship involves two specified, different document types.
     *
     * @param relationDocModel a document model for a Relation record.
     * @param desiredDocType a desired document type.
     * @param relatedDocType a related document type.
     * @throws ClientException
     * @return the CSID from the desired document type in the relation. Returns
     * an empty string if the Relation record does not involve both the desired
     * and related document types, or if the desired document type is at both
     * ends of the relation.
     */
    protected static String getCsidForDesiredDocType(DocumentModel relationDocModel,
            String desiredDocType, String relatedDocType) throws ClientException {
        String csid = "";
        String subjectDocType = (String) relationDocModel.getProperty(RELATIONS_COMMON_SCHEMA, SUBJECT_DOCTYPE_PROPERTY);
        String objectDocType = (String) relationDocModel.getProperty(RELATIONS_COMMON_SCHEMA, OBJECT_DOCTYPE_PROPERTY);
        if (subjectDocType.startsWith(desiredDocType) && objectDocType.startsWith(desiredDocType)) {
            return csid;
        }
        if (subjectDocType.startsWith(desiredDocType) && objectDocType.startsWith(relatedDocType)) {
            csid = (String) relationDocModel.getProperty(RELATIONS_COMMON_SCHEMA, SUBJECT_CSID_PROPERTY);
        } else if (subjectDocType.startsWith(relatedDocType) && objectDocType.startsWith(desiredDocType)) {
            csid = (String) relationDocModel.getProperty(RELATIONS_COMMON_SCHEMA, OBJECT_CSID_PROPERTY);
        }
        return csid;
    }

    // Can be extended by sub-classes to update different/multiple values;
    // e.g. values for moveable locations ("crates").
    /**
     * Updates a CollectionObject record with selected values from a Movement
     * record.
     *
     * @param collectionObjectDocModel a document model for a CollectionObject
     * record.
     * @param movementDocModel a document model for a Movement record.
     * @throws ClientException
     */
    protected abstract DocumentModel updateCollectionObjectValuesFromMovement(DocumentModel collectionObjectDocModel,
            DocumentModel movementDocModel)
            throws ClientException;
}