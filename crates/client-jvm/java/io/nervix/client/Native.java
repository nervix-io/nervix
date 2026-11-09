package io.nervix.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** The JNI declarations used only by Cleaner-owned public handles. */
final class Native {
    static { load(); }
    private Native() {}

    private static void load() {
        String explicit = System.getProperty("io.nervix.client.library");
        if (explicit != null) {
            System.load(Path.of(explicit).toAbsolutePath().toString());
            return;
        }
        String filename = System.mapLibraryName("nervix_client_jvm");
        try (var resource = Native.class.getResourceAsStream("native/" + filename)) {
            if (resource == null) {
                System.loadLibrary("nervix_client_jvm");
                return;
            }
            Path directory = Files.createTempDirectory("nervix-jni-");
            directory.toFile().deleteOnExit();
            Path library = directory.resolve(filename);
            library.toFile().deleteOnExit();
            Files.copy(resource, library, StandardCopyOption.REPLACE_EXISTING);
            System.load(library.toAbsolutePath().toString());
        } catch (IOException failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    static native long fieldsNew();
    static native void fieldsAdd(long handle, String name, int kind, long length, boolean nullable, boolean sensitive);
    static native void fieldsElement(long handle, int kind, long length);
    static native long fieldsSchema(long handle);
    static native Object[] schemaFields(long handle, int part);
    static native String schemaBranch(long handle);
    static native long builderNew(long schema, int rows);
    static native void builderStates(long handle, int column, byte[] states);
    static native void builderOffsets(long handle, int column, int level, long[] offsets);
    static native void builderFixed(long handle, int column, int level, byte[] data);
    static native void builderVarlen(long handle, int column, int level, long[] offsets, byte[] data);
    static native long builderFinish(long handle);
    static native long batchFromIpc(long schema, int rows, byte[] ipc);
    static native int batchRows(long handle);
    static native long batchSchema(long handle);
    static native byte[] batchIpc(long handle);
    static native int batchCells(long handle, int column, int level);
    static native byte[] batchStates(long handle, int column);
    static native long[] batchOffsets(long handle, int column, int level);
    static native byte[] batchFixed(long handle, int column, int level);
    static native Object batchVarlen(long handle, int column, int level);
    static native int clockEventKind(long handle);
    static native String clockEventDomain(long handle);
    static native long clockEventGeneration(long handle);
    static native int clockEventState(long handle);
    static native Object clockEventPaced(long handle);
    static native Object clockEventTick(long handle);
    static native int clockEventEnd(long handle);
    static native Object clockEventRestoration(long handle);
    static native String clockDomain(long handle);
    static native long clockGeneration(long handle);
    static native int clockState(long handle);
    static native Object clockPaced(long handle);
    static native Object clockTick(long handle);
    static native long clockLogical(long handle, long utc);
    static native long clockWait(long handle, long utc, long target);
    static native long[] clockWindow(long handle, long utc);
    static native boolean clockAdmits(long handle, long utc, long event);
    static native long suggest(long session, String input, int cursor, int page_size, String continuation, long token);
    static native int suggestionsStatus(long handle);
    static native String suggestionsContinuation(long handle);
    static native Object[] suggestionsValues(long handle);
    static native long producerOpen(long session, String domain, String name, long fields, int batches, long bytes, long token);
    static native long consumerOpen(long session, String domain, String name, long fields, int batches, long bytes, long token);
    static native long producerSchema(long handle);
    static native long consumerSchema(long handle);
    static native int producerState(long handle);
    static native int consumerState(long handle);
    static native int producerAdmission(long handle);
    static native int[] producerReopen(long handle);
    static native int[] consumerReopen(long handle);
    static native Object producerDescription(long handle);
    static native Object consumerDescription(long handle);
    static native long producerSubmit(long handle, long batch, long token);
    static native long producerSubmitIpc(long handle, byte[] ipc, long token);
    static native long producerRejoin(long handle, long id, long token);
    static native long producerRelease(long handle, long id);
    static native void producerClose(long handle, long token);
    static native void consumerClose(long handle, long token);
    static native long consumerNext(long handle, long token);
    static native byte[] deliveryIdentity(long handle);
    static native byte[] deliveryReference(long handle);
    static native String deliverySource(long handle);
    static native byte[] deliveryBranch(long handle);
    static native long deliveryMembers(long handle);
    static native long deliveryNow(long handle);
    static native byte[] deliveryIpc(long handle);
    static native long deliveryBatch(long handle);
    static native int deliveryAck(long handle, long token);
    static native int deliveryRetry(long handle, long token);
    static native int deliveryReject(long handle, String reason, long token);
    static native Object submissionInfo(long handle);
    static native Object[] producerPending(long handle);
    static native int eventKind(long handle);
    static native long eventRows(long handle);
    static native Object eventSubscription(long handle);
    static native long eventSchema(long handle);
    static native long eventBatch(long handle);
    static native Object eventRestoration(long handle);
    static native int eventEnd(long handle);
    static native Object buffer(long handle);
    static native byte[] eventStates(long handle, int part, int column);
    static native byte[] eventFixed(long handle, int part, int column);
    static native Object eventVarlen(long handle, int part, int column);
    static native byte[] eventCell(long handle, int part, int row, int column);
    static native long retain(long handle);
    static native void release(long handle);
    static native String executionReference(long handle);
    static native long optionsNew();
    static native void optionsAuth(long handle, String username, String password);
    static native void optionsTls(long handle, int requirement);
    static native void optionsCa(long handle, byte[] pem);
    static native void optionsSeeds(long handle, Object[] seeds);
    static native void optionsTimeout(long handle, int kind, double value);
    static native long cancelNew();
    static native long cancelDeadline(double value);
    static native void cancelTrigger(long handle);
    static native long connect(String server, String domain, long options, long token);
    static native long prepare(long session, String query, long token);
    static native long execute(long session, long execution, long token);
    static native long nextEvent(long session, long token);
    static native long nextClockEvent(long session, long token);
    static native long domainClock(long session, String domain);
    static native int outcomeDisposition(long handle);
    static native int outcomeUploadFailure(long handle);
    static native String outcomeMessage(long handle);
    static native String outcomeReference(long handle);
    static native long outcomeSchema(long handle);
    static native Object outcomeSubscription(long handle);
    static native Object[] outcomeDiagnostics(long handle);
    static native Object outcomeBackup(long handle);
    static native Object outcomeRestore(long handle);
    static native long outcomeInspection(long handle);
    static native String inspectionDomain(long handle);
    static native String inspectionEntityKind(long handle);
    static native String inspectionEntityName(long handle);
    static native String inspectionJson(long handle);
    static native int inspectionRejection(long handle);
    static native Object inspectionCounts(long handle);
    static native Object inspectionUnitCount(long handle);
    static native String inspectionContinuation(long handle);
    static native int inspectionUnitsLen(long handle);
    static native int inspectionUnitKind(long handle, int unit);
    static native Object inspectionUnitIndex(long handle, int unit);
    static native String inspectionUnitOwner(long handle, int unit);
    static native Object[] inspectionUnitReplicas(long handle, int unit);
    static native String inspectionUnitOriginNode(long handle, int unit);
    static native long inspectionUnitOriginIncarnation(long handle, int unit);
    static native byte[] inspectionUnitOriginFingerprint(long handle, int unit);
    static native long inspectionUnitOriginGeneration(long handle, int unit);
    static native long inspectionUnitOriginRevision(long handle, int unit);
    static native int inspectionUnitOriginStage(long handle, int unit);
    static native Object inspectionUnitCounts(long handle, int unit);
    static native int inspectionSectionsLen(long handle, int unit);
    static native String inspectionSectionName(long handle, int unit, int section);
    static native long inspectionSectionOmittedRows(long handle, int unit, int section);
    static native long[] inspectionSectionWatermarks(long handle, int unit, int section);
    static native long inspectionSectionSchema(long handle, int unit, int section);
    static native int inspectionFramesLen(long handle, int unit, int section);
    static native long inspectionFrame(long handle, int unit, int section, int frame);
    static native long inspectionFrameRows(long handle);
    static native long inspectionFrameSchema(long handle);
}
