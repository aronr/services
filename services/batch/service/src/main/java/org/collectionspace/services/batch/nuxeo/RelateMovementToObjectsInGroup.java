package org.collectionspace.services.batch.nuxeo;

import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.PathSegment;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.collectionspace.services.batch.AbstractBatchInvocable;
import org.collectionspace.services.client.AbstractCommonListUtils;
import org.collectionspace.services.client.CollectionObjectClient;
import org.collectionspace.services.client.IQueryManager;
import org.collectionspace.services.client.MovementClient;
import org.collectionspace.services.client.PoxPayloadOut;
import org.collectionspace.services.client.RelationClient;
import org.collectionspace.services.client.workflow.WorkflowClient;
import org.collectionspace.services.common.ResourceBase;
import org.collectionspace.services.common.ResourceMap;
import org.collectionspace.services.common.api.Tools;
import org.collectionspace.services.common.invocable.InvocationResults;
import org.collectionspace.services.common.relation.RelationJAXBSchema;
import org.collectionspace.services.jaxb.AbstractCommonList;
import org.collectionspace.services.jaxb.AbstractCommonList.ListItem;
import org.dom4j.DocumentException;
import org.jboss.resteasy.specimpl.UriInfoImpl;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelateMovementToObjectsInGroup extends AbstractBatchInvocable {

    // FIXME: Where appropriate, get from existing constants rather than local declarations
    private final static String LIFECYCLE_STATE_ELEMENT_NAME = "currentLifeCycleState";
    private final static String WORKFLOW_COMMON_SCHEMA_NAME = "workflow_common";
    private final static String WORKFLOW_COMMON_NAMESPACE_PREFIX = "ns2";
    private final static String WORKFLOW_COMMON_NAMESPACE_URI =
            "http://collectionspace.org/services/workflow";
    private final static Namespace WORKFLOW_COMMON_NAMESPACE =
            Namespace.getNamespace(
            WORKFLOW_COMMON_NAMESPACE_PREFIX,
            WORKFLOW_COMMON_NAMESPACE_URI);
    private final String COLLECTIONOBJECT_DOCTYPE = "CollectionObject";
    private final String MOVEMENT_DOCTYPE = "Movement";
    private final boolean EXCLUDE_DELETED = true;
    private final String RELATION_TYPE = "affects";
    private final String RELATION_PREDICATE_DISPLAY_NAME = RELATION_TYPE;
    String errMsg;
    private final String CLASSNAME = this.getClass().getSimpleName();
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    // Initialization tasks
    public RelateMovementToObjectsInGroup() {
        setSupportedInvocationModes(Arrays.asList(INVOCATION_MODE_SINGLE));
    }

    /**
     * The main work logic of the batch job. Will be called after setContext.
     */
    @Override
    public void run() {

        setCompletionStatus(STATUS_MIN_PROGRESS);

        try {

            Set<String> groupMovementCsids = new HashSet<>();
            Set<String> groupCollectionObjectCsids = new HashSet<>();

            if (requestIsForInvocationModeSingle()) {
                String groupCsid = getInvocationContext().getSingleCSID();
                if (Tools.isBlank(groupCsid)) {
                    throw new Exception(CSID_VALUES_NOT_PROVIDED_IN_INVOCATION_CONTEXT);
                }
                // Build a list of Movement records related to this group.
                // If just a single Movement record is related to the group, it now
                // becomes the relevant Movement record, to which CollectionObjects
                // also belonging to this group can be related.
                groupMovementCsids = getMemberCsidsFromGroup(MovementClient.SERVICE_NAME, groupCsid);
                if (groupMovementCsids.isEmpty()) {
                    errMsg = "Could not move the objects in this group: no Movement record is currently related to this group";
                    throw new Exception(errMsg);
                } else if (groupMovementCsids.size() > 1) {
                    errMsg = "Could not move the objects in this group: more than one Movement record is related to this group";
                    throw new Exception(errMsg);
                }
                // Build a list of CollectionObject records related to this group.
                groupCollectionObjectCsids = getMemberCsidsFromGroup(CollectionObjectClient.SERVICE_NAME, groupCsid);
                if (groupCollectionObjectCsids.isEmpty()) {
                    errMsg = "Could not move the objects in this group: no objects were found";
                    throw new Exception(errMsg);
                }
            }
            if (logger.isInfoEnabled()) {
                logger.info("Identified " + groupCollectionObjectCsids.size() + " total CollectionObject(s) to be processed via the " + CLASSNAME + " batch job");
            }

            // Relate each CollectionObject record to the relevant Movement record,
            // if those relations do not already exist.
            setResults(relateCollectionObjectsToMovement(groupCollectionObjectCsids, groupMovementCsids.iterator().next()));
            setCompletionStatus(STATUS_COMPLETE);

        } catch (Exception e) {
            errMsg = "Error encountered in " + CLASSNAME + ": " + e.getMessage();
            setErrorResult(e.getMessage());
            getResults().setNumAffected(0);
        }

    }

    private InvocationResults relateCollectionObjectsToMovement(Set<String> collectionObjectCsids, String movementCsid) {
        ResourceMap resourceMap = getResourceMap();
        ResourceBase collectionObjectResource = resourceMap.get(CollectionObjectClient.SERVICE_NAME);
        ResourceBase movementResource = resourceMap.get(MovementClient.SERVICE_NAME);
        String subjectCsid;
        String objectCsid;
        boolean objectAlreadyRelatedToMovement;
        int numUpdated = 0;

        try {

            // For each CollectionObject record
            for (String collectionObjectCsid : collectionObjectCsids) {

                // FIXME: Optionally set competition status here to
                // indicate what percentage of records have been processed.

                // If this CollectionObject record is soft-deleted, skip it
                // and go onto the next record
                if (isRecordDeleted(collectionObjectResource, collectionObjectCsid)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Skipping soft-deleted CollectionObject record with CSID " + collectionObjectCsid);
                    }
                    continue;
                }
                // Get all Movement records related to this CollectionObject record
                AbstractCommonList relatedMovements =
                        getRelatedRecords(movementResource, collectionObjectCsid, EXCLUDE_DELETED);
                List<ListItem> relatedMovementItems = relatedMovements.getListItem();
                // If this CollectionObject is related to one or more
                // Movement records, check whether it might already
                // be related to the relevant Movement record.
                // If it is, avoid creating a duplicate relation by
                // skipping it and going onto the next record.
                if (!relatedMovementItems.isEmpty()) {
                    objectAlreadyRelatedToMovement = false;
                    for (ListItem relatedMovementItem : relatedMovementItems) {
                        subjectCsid = AbstractCommonListUtils.ListItemGetElementValue(relatedMovementItem, RelationJAXBSchema.SUBJECT_CSID);
                        if (Tools.notBlank(subjectCsid) && subjectCsid.equals(movementCsid)) {
                            objectAlreadyRelatedToMovement = true;
                            break;
                        }
                        objectCsid = AbstractCommonListUtils.ListItemGetElementValue(relatedMovementItem, RelationJAXBSchema.OBJECT_CSID);
                        if (Tools.notBlank(objectCsid) && objectCsid.equals(movementCsid)) {
                            objectAlreadyRelatedToMovement = true;
                            break;
                        }
                    }
                    if (objectAlreadyRelatedToMovement) {
                        continue;
                    }
                }
                // Relate this CollectionObject record to the relevant Movement record
                numUpdated = relateCollectionObjectToMovement(collectionObjectCsid,
                        movementCsid, resourceMap, numUpdated);
            }

        } catch (Exception e) {
            errMsg = "Error encountered in " + CLASSNAME + ": " + e.getMessage() + " ";
            errMsg = errMsg + "Successfully related " + numUpdated + " object(s) to movement prior to error";
            logger.error(errMsg);
            setErrorResult(errMsg);
            getResults().setNumAffected(numUpdated);
            return getResults();
        }

        String successMsg = "Related " + numUpdated + " object(s) to movement";
        logger.info(successMsg);
        getResults().setUserNote(successMsg);
        getResults().setNumAffected(numUpdated);
        return getResults();
    }

    /**
     * Create reciprocal relation records, relating a CollectionObject record to
     * a Movement record.
     *
     * @param collectionObjectCsid
     * @param movementCsid
     * @param resourceMap
     * @param numUpdated
     * @return numUpdated
     * @throws Exception
     */
    private int relateCollectionObjectToMovement(String collectionObjectCsid,
            String movementCsid, ResourceMap resourceMap, int numUpdated) throws Exception {

        ResourceBase resource;
        Response response;

        String collectionObjectMovementRelationPayload =
                buildRelationPayload(collectionObjectCsid, COLLECTIONOBJECT_DOCTYPE, movementCsid, MOVEMENT_DOCTYPE);
        resource = resourceMap.get(RelationClient.SERVICE_NAME);
        response = resource.create(resourceMap, null, collectionObjectMovementRelationPayload);
        if (response.getStatus() != CREATED_STATUS) {
            errMsg = "Could not create relation record between object and movement";
            throw new Exception(errMsg);
        }

        String movementCollectionObjectRelationPayload =
                buildRelationPayload(movementCsid, MOVEMENT_DOCTYPE, collectionObjectCsid, COLLECTIONOBJECT_DOCTYPE);
        resource = resourceMap.get(RelationClient.SERVICE_NAME);
        response = resource.create(resourceMap, null, movementCollectionObjectRelationPayload);
        if (response.getStatus() != CREATED_STATUS) {
            errMsg = "Could not create relation record between movement and object";
            throw new Exception(errMsg);
        }

        return ++numUpdated;
    }

    private String buildRelationPayload(String subjectCsid, String subjectDoctype, String objectCsid, String objectDoctype) {
        return "<document name=\"relations\">"
                + "<ns2:relations_common xmlns:ns2=\"http://collectionspace.org/services/relation\""
                + " xmlns:ns3=\"http://collectionspace.org/services/jaxb\">"
                + "<subjectCsid>" + subjectCsid + "</subjectCsid>"
                + "<subjectDocumentType>" + subjectDoctype + "</subjectDocumentType>"
                + "<objectCsid>" + objectCsid + "</objectCsid>"
                + "<objectDocumentType>" + objectDoctype + "</objectDocumentType>"
                + "<relationshipType>" + RELATION_TYPE + "</relationshipType>"
                + "<predicateDisplayName>" + RELATION_PREDICATE_DISPLAY_NAME + "</predicateDisplayName>"
                + "</ns2:relations_common></document>";
    }

    // #################################################################
    // Ray Lee's convenience methods from his AbstractBatchJob class for the
    // UC Berkeley Botanical Garden v2.4 implementation.
    // #################################################################
    // FIXME: Once these methods are moved to framework class(es),
    // remove their declarations below and import their new class(es).
    protected PoxPayloadOut findByCsid(String serviceName, String csid) throws URISyntaxException, DocumentException {
        ResourceBase resource = getResourceMap().get(serviceName);
        return findByCsid(resource, csid);
    }

    protected PoxPayloadOut findByCsid(ResourceBase resource, String csid) throws URISyntaxException, DocumentException {
        byte[] response = resource.get(null, createUriInfo(), csid);
        PoxPayloadOut payload = new PoxPayloadOut(response);
        return payload;
    }

    protected UriInfo createUriInfo() throws URISyntaxException {
        return createUriInfo("");
    }

    protected UriInfo createUriInfo(String queryString) throws URISyntaxException {
        URI absolutePath = new URI("");
        URI baseUri = new URI("");
        return new UriInfoImpl(absolutePath, baseUri, "", queryString, Collections.<PathSegment>emptyList());
    }

    // #################################################################
    // Other convenience methods
    // #################################################################
    protected UriInfo createRelatedRecordsUriInfo(String queryString) throws URISyntaxException {
        URI uri = new URI(null, null, null, queryString, null);
        return createUriInfo(uri.getRawQuery());
    }

    protected String getFieldElementValue(PoxPayloadOut payload, String partLabel, Namespace partNamespace, String fieldPath) {
        String value = null;
        SAXBuilder builder = new SAXBuilder();
        try {
            Document document = builder.build(new StringReader(payload.toXML()));
            Element root = document.getRootElement();
            // The part element is always expected to have an explicit namespace.
            Element part = root.getChild(partLabel, partNamespace);
            // Try getting the field element both with and without a namespace.
            // Even though a field element that lacks a namespace prefix
            // may yet inherit its namespace from a parent, JDOM may require that
            // the getChild() call be made without a namespace.
            Element field = part.getChild(fieldPath, partNamespace);
            if (field == null) {
                field = part.getChild(fieldPath);
            }
            if (field != null) {
                value = field.getText();
            }
        } catch (Exception e) {
            logger.error("Error getting value from field path " + fieldPath
                    + " in schema part " + partLabel);
            return null;
        }
        return value;
    }

    private boolean isRecordDeleted(ResourceBase resource, String collectionObjectCsid)
            throws URISyntaxException, DocumentException {
        boolean isDeleted = false;
        byte[] workflowResponse = resource.getWorkflow(createUriInfo(), collectionObjectCsid);
        if (workflowResponse != null) {
            PoxPayloadOut payloadOut = new PoxPayloadOut(workflowResponse);
            String workflowState =
                    getFieldElementValue(payloadOut, WORKFLOW_COMMON_SCHEMA_NAME,
                    WORKFLOW_COMMON_NAMESPACE, LIFECYCLE_STATE_ELEMENT_NAME);
            if (Tools.notBlank(workflowState) && workflowState.equals(WorkflowClient.WORKFLOWSTATE_DELETED)) {
                isDeleted = true;
            }
        }
        return isDeleted;
    }

    private UriInfo addFilterToExcludeSoftDeletedRecords(UriInfo uriInfo) throws URISyntaxException {
        if (uriInfo == null) {
            uriInfo = createUriInfo();
        }
        uriInfo.getQueryParameters().add(WorkflowClient.WORKFLOW_QUERY_NONDELETED, Boolean.FALSE.toString());
        return uriInfo;
    }

    private AbstractCommonList getRecordsRelatedToCsid(ResourceBase resource, String csid,
            String relationshipDirection, boolean excludeDeletedRecords) throws URISyntaxException {
        UriInfo uriInfo = createUriInfo();
        uriInfo.getQueryParameters().add(relationshipDirection, csid);
        if (excludeDeletedRecords) {
            uriInfo = addFilterToExcludeSoftDeletedRecords(uriInfo);
        }
        // The 'resource' type used here identifies the record type of the
        // related records to be retrieved
        AbstractCommonList relatedRecords = resource.search(uriInfo,null,null,null,null);
        if (logger.isTraceEnabled()) {
            logger.trace("Identified " + relatedRecords.getTotalItems()
                    + " record(s) related to the object record via direction " + relationshipDirection + " with CSID " + csid);
        }
        return relatedRecords;
    }

    /**
     * Returns the records of a specified type that are related to a specified
     * record, where that record is the object of the relation.
     *
     * @param resource a resource. The type of this resource determines the type
     * of related records that are returned.
     * @param csid a CSID identifying a record
     * @param excludeDeletedRecords true if 'soft-deleted' records should be
     * excluded from results; false if those records should be included
     * @return a list of records of a specified type, related to a specified
     * record
     * @throws URISyntaxException
     */
    private AbstractCommonList getRecordsRelatedToObjectCsid(ResourceBase resource, String csid, boolean excludeDeletedRecords) throws URISyntaxException {
        return getRecordsRelatedToCsid(resource, csid, IQueryManager.SEARCH_RELATED_TO_CSID_AS_OBJECT, excludeDeletedRecords);
    }

    /**
     * Returns the records of a specified type that are related to a specified
     * record, where that record is the subject of the relation.
     *
     * @param resource a resource. The type of this resource determines the type
     * of related records that are returned.
     * @param csid a CSID identifying a record
     * @param excludeDeletedRecords true if 'soft-deleted' records should be
     * excluded from results; false if those records should be included
     * @return a list of records of a specified type, related to a specified
     * record
     * @throws URISyntaxException
     */
    private AbstractCommonList getRecordsRelatedToSubjectCsid(ResourceBase resource, String csid, boolean excludeDeletedRecords) throws URISyntaxException {
        return getRecordsRelatedToCsid(resource, csid, IQueryManager.SEARCH_RELATED_TO_CSID_AS_SUBJECT, excludeDeletedRecords);
    }

    private AbstractCommonList getRelatedRecords(ResourceBase resource, String csid, boolean excludeDeletedRecords)
            throws URISyntaxException, DocumentException {
        AbstractCommonList relatedRecords = new AbstractCommonList();
        AbstractCommonList recordsRelatedToObjectCSID = getRecordsRelatedToObjectCsid(resource, csid, excludeDeletedRecords);
        AbstractCommonList recordsRelatedToSubjectCSID = getRecordsRelatedToSubjectCsid(resource, csid, excludeDeletedRecords);
        // If either list contains any related records, merge in its items
        if (recordsRelatedToObjectCSID.getListItem().size() > 0) {
            relatedRecords.getListItem().addAll(recordsRelatedToObjectCSID.getListItem());
        }
        if (recordsRelatedToSubjectCSID.getListItem().size() > 0) {
            relatedRecords.getListItem().addAll(recordsRelatedToSubjectCSID.getListItem());
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Identified a total of " + relatedRecords.getListItem().size()
                    + " record(s) related to the record with CSID " + csid);
        }
        return relatedRecords;
    }

    private Set<String> getCsidsList(AbstractCommonList list) {
        Set<String> csids = new HashSet<>();
        for (AbstractCommonList.ListItem listitem : list.getListItem()) {
            csids.add(AbstractCommonListUtils.ListItemGetCSID(listitem));
        }
        return csids;
    }

    private Set<String> getMemberCsidsFromGroup(String serviceName, String groupCsid) throws URISyntaxException, DocumentException {
        ResourceMap resourcemap = getResourceMap();
        ResourceBase resource = resourcemap.get(serviceName);
        return getMemberCsidsFromGroup(resource, groupCsid);
    }

    private Set<String> getMemberCsidsFromGroup(ResourceBase resource, String groupCsid) throws URISyntaxException, DocumentException {
        // The 'resource' type used here identifies the record type of the
        // related records to be retrieved
        AbstractCommonList relatedRecords =
                getRelatedRecords(resource, groupCsid, EXCLUDE_DELETED);
        Set<String> memberCsids = getCsidsList(relatedRecords);
        return memberCsids;
    }
}
