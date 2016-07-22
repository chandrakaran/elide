/*
 * Copyright 2016, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.security;

import com.yahoo.elide.core.filter.expression.FilterExpression;
import org.antlr.v4.runtime.tree.ParseTree;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Interface describing classes responsible for managing the life-cycle and execution of checks.
 *
 * Checks are expected to throw exceptions upon failures.
 */
public interface PermissionExecutor {
    /**
     * Check permission on class.
     *
     * @param annotationClass annotation class
     * @param resource resource
     * @param <A> type parameter
     * @see com.yahoo.elide.annotation.CreatePermission
     * @see com.yahoo.elide.annotation.ReadPermission
     * @see com.yahoo.elide.annotation.UpdatePermission
     * @see com.yahoo.elide.annotation.DeletePermission
     */
    <A extends Annotation> void checkPermission(Class<A> annotationClass, PersistentResource resource);

    /**
     * Check permission on class.
     *
     * @param annotationClass annotation class
     * @param resource resource
     * @param changeSpec ChangeSpec
     * @param <A> type parameter
     * @see com.yahoo.elide.annotation.CreatePermission
     * @see com.yahoo.elide.annotation.ReadPermission
     * @see com.yahoo.elide.annotation.UpdatePermission
     * @see com.yahoo.elide.annotation.DeletePermission
     */
    <A extends Annotation> void checkPermission(Class<A> annotationClass,
                                                PersistentResource resource,
                                                ChangeSpec changeSpec);

    /**
     * Check for permissions on a specific field.
     *
     * @param resource resource
     * @param changeSpec changepsec
     * @param annotationClass annotation class
     * @param field field to check
     * @param <A> type parameter
     */
    <A extends Annotation> void checkSpecificFieldPermissions(PersistentResource<?> resource,
                                                              ChangeSpec changeSpec,
                                                              Class<A> annotationClass,
                                                              String field);

    /**
     * Check for permissions on a specific field deferring all checks.
     *
     * @param resource resource
     * @param changeSpec changepsec
     * @param annotationClass annotation class
     * @param field field to check
     * @param <A> type parameter
     */
    <A extends Annotation> void checkSpecificFieldPermissionsDeferred(PersistentResource<?> resource,
                                                                      ChangeSpec changeSpec,
                                                                      Class<A> annotationClass,
                                                                      String field);

    /**
     * Check strictly user permissions on a specific field and entity.
     *
     * @param resource Resource
     * @param annotationClass Annotation class
     * @param field Field
     * @param <A> type parameter
     */
    <A extends Annotation> void checkUserPermissions(PersistentResource<?> resource,
                                                     Class<A> annotationClass,
                                                     String field);

    /**
     * Check strictly user permissions on an entity.
     *
     * @param resourceClass Resource class
     * @param annotationClass Annotation class
     * @param <A> type parameter
     */
    <A extends Annotation> void checkUserPermissions(Class<?> resourceClass, Class<A> annotationClass);

    Optional<FilterExpression> getReadPermissionFilter(Class<?> resourceClass);

    /**
     * Execute commmit checks.
     */
    void executeCommitChecks();

    /**
     * Method to build criterion check.
     *
     * @param permissions Permissions to visit
     * @param criterionNegater Function to apply negation to a criterion
     * @param andCriterionJoiner Function to combine criteria with an and condition
     * @param orCriterionJoiner Function to combine criteria with an or condition
     * @param <T> type parameter
     * @return Built criterion if possible else null. Default always returns null.
     */
    default <T> T getCriterion(ParseTree permissions,
                               Function<T, T> criterionNegater,
                               BiFunction<T, T, T> andCriterionJoiner,
                               BiFunction<T, T, T> orCriterionJoiner) {
        return null;
    }

    /**
     * Whether or not the permission executor will return verbose logging to the requesting user in the response.
     *
     * @return True if verbose, false otherwise.
     */
    default boolean isVerbose() {
        return false;
    }
}
